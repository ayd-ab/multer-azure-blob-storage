// *********************************************************
//
// This file is subject to the terms and conditions defined in the
// file 'LICENSE.txt', which is part of this source code package.
//
// *********************************************************

// Node Modules
import { extname } from "node:path";
import { v4 } from "uuid";
import { Request } from "express";
import { StorageEngine } from "multer";
import { BlobSASPermissions, BlobServiceClient, StorageSharedKeyCredential } from '@azure/storage-blob';

// Custom types
export interface MASContentSettings {
    cacheControl?: string;
    /** Optional. Sets the blob's content type. If specified, this property is stored with the blob and returned with a read request. */
    contentType?: string;
    /** Optional. An MD5 hash of the blob content. Note that this hash is not validated, as the hashes for the individual blocks were validated when each was uploaded. */
    contentMD5?: Uint8Array;
    /** Optional. Sets the blob's content encoding. If specified, this property is stored with the blob and returned with a read request. */
    contentEncoding?: string;
    /** Optional. Set the blob's content language. If specified, this property is stored with the blob and returned with a read request. */
    contentLanguage?: string;
    /** Optional. Sets the blob's Content-Disposition header. */
    contentDisposition?: string;
}

export type MASMetadata = { [k: string]: string };

export type MASResolverFunction<T> = (req: Request, file: Express.Multer.File) => Promise<T>;

export enum MASContainerAccessLevel {
    PublicContainer = 'container',
    PublicBlob = 'blob',
    Private = 'private'
}

// Custom interfaces
export interface IMASOptions {
    accessKey?: string;
    accountName?: string;
    connectionString?: string;
    urlExpirationTime?: number | false;
    blobName?: MASResolverFunction<string>;
    containerName: MASResolverFunction<string> | string;
    autoCreateContainer?: boolean;
    metadata?: MASResolverFunction<MASMetadata> | MASMetadata;
    contentSettings?: MASResolverFunction<MASContentSettings> | MASContentSettings;
    containerAccessLevel?: MASContainerAccessLevel;
}

export interface MulterOutFile extends Express.Multer.File {
    url: string;
    etag?: string;
    metadata?: any;
    containerName: string;
    blobName: string;
    blobType?: string;
    blobSize?: number;
}

const DEFAULT_CONTAINER_ACCESS_LEVEL = MASContainerAccessLevel.Private;
const DEFAULT_URL_EXPIRATION_TIME: number = 60; // Minutes

const promisifyValue = <T>(value: T): MASResolverFunction<T> => () => Promise.resolve(value);

const promisifyOption = <T>(value: T | MASResolverFunction<T>): MASResolverFunction<T> =>
  typeof value === 'function' ? (value as unknown as MASResolverFunction<T>) : promisifyValue(value);

const generateBlobName = (_req: Request, file: Express.Multer.File): Promise<string> => {
    return new Promise<string>((resolve, _reject) => {
        resolve(`${Date.now()}-${v4()}${extname(file.originalname)}`);
    });
};

export class MulterAzureStorage implements StorageEngine {
    private _blobServiceClient: BlobServiceClient;
    private _blobName: MASResolverFunction<string>;
    private _urlExpirationTime: number | null;
    private _metadata: MASResolverFunction<MASMetadata> | null;
    private _contentSettings: MASResolverFunction<MASContentSettings> | null;
    private _containerName: MASResolverFunction<string>;
    private _containerAccessLevel: MASContainerAccessLevel;
    private _autoCreateContainer: boolean;

    constructor(options: IMASOptions) {
        // Connection credentials are required
        const connectionString = options.connectionString || process.env.AZURE_STORAGE_CONNECTION_STRING || null;
        const accessKey = options.accessKey || process.env.AZURE_STORAGE_ACCESS_KEY || null;
        const accountName = options.accountName || process.env.AZURE_STORAGE_ACCOUNT || null;

        let connection: string | { accountName: string, accessKey: string } | null = null;
        if (connectionString) {
            connection = connectionString;
        } else if (accessKey && accountName) {
            connection = { accountName, accessKey };
        }
        if (connection == null) {
            throw new Error("Missing required parameter: Either the connection string or account name and access key must be provided.");
        }
        if (!options.containerName) {
            throw new Error("Missing required parameter: Azure container name.");
        }
        // Set the proper container name
        this._containerName = promisifyOption(options.containerName);
        // Set container access level - Private by default
        this._containerAccessLevel = options.containerAccessLevel ?? DEFAULT_CONTAINER_ACCESS_LEVEL;
        // Check for metadata
        this._metadata = options.metadata ? promisifyOption(options.metadata) : null;
        // Check for content settings
        this._contentSettings = options.contentSettings ? promisifyOption(options.contentSettings) : null;
        // Set the proper blob name
        this._blobName = options.blobName ? options.blobName : generateBlobName;
        // Set url expiration time
        if (options.urlExpirationTime === false) {
            this._urlExpirationTime = null;
        } else if (options.urlExpirationTime == null || options.urlExpirationTime <= 0) {
            this._urlExpirationTime = DEFAULT_URL_EXPIRATION_TIME;
        } else {
            this._urlExpirationTime = options.urlExpirationTime;
        }
        // Check for auto container creation
        this._autoCreateContainer = options.autoCreateContainer ?? true;
        // Init blob service
        if (typeof connection === 'string') {
            this._blobServiceClient = BlobServiceClient.fromConnectionString(connection);
        } else {
            const credentials = new StorageSharedKeyCredential(connection.accountName, connection.accessKey);
            this._blobServiceClient = new BlobServiceClient(`https://${options.accountName}.blob.core.windows.net`, credentials);
        }
    }

    _handleFile(req: Request, file: Express.Multer.File, cb: (error?: any, info?: Partial<Express.Multer.File>) => void) {
        this._handleFileAsync(req, file).then(file => cb(null, file)).catch(err => cb(err));
    }

    _removeFile(req: Request, file: Express.Multer.File, cb: (error: Error | null) => void) {
        this._removeFileAsync(req, file).then(() => cb(null)).catch(err => cb(err));
    }


    /** Helpers */

    private async _handleFileAsync(req: Request, file: Express.Multer.File): Promise<MulterOutFile> {
        // Resolve blob name and container name
        const containerName: string = await this._containerName(req, file);
        const blobName: string = await this._blobName(req, file);
        const contentSettings = this._contentSettings ? await this._contentSettings(req, file) : {
            contentType: file.mimetype,
            contentDisposition: 'inline'
        };
        const metadata = this._metadata && await this._metadata(req, file);
        const containerClient = await this._getContainerClient(containerName);
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        await blockBlobClient.uploadStream(file.stream, undefined, undefined, {
            metadata: metadata || undefined,
            blobHTTPHeaders: {
                blobCacheControl: contentSettings?.cacheControl,
                blobContentType: contentSettings?.contentType,
                blobContentMD5: contentSettings?.contentMD5,
                blobContentEncoding: contentSettings?.contentEncoding,
                blobContentLanguage: contentSettings.contentLanguage,
                blobContentDisposition: contentSettings.contentDisposition,
            }
        });
        // Create the output file
        const blobProperties = await blockBlobClient.getProperties();
        const sasUrl = await blockBlobClient.generateSasUrl({
            expiresOn: this._urlExpirationTime ? new Date(Date.now() + this._urlExpirationTime * 60 * 1000) : undefined,
            permissions:  BlobSASPermissions.from({ read: true }),
        });
        return {
            ...file,
            url: sasUrl,
            etag: blobProperties.etag,
            metadata: blobProperties.metadata,
            containerName: containerName,
            blobName: blobName,
            blobType: blobProperties.blobType,
            blobSize: blobProperties.contentLength,
        };
    }

    private async _removeFileAsync(req: Request, file: Express.Multer.File) {
        const containerName: string = await this._containerName(req, file);
        const blobName: string = await this._blobName(req, file);
        const containerClient = await this._getContainerClient(containerName);
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        await blockBlobClient.deleteIfExists();
    }

    private async _getContainerClient(containerName: string) {
        const containerClient = this._blobServiceClient.getContainerClient(containerName);
        if (this._autoCreateContainer) {
            const publicAccessLevel = this._containerAccessLevel === MASContainerAccessLevel.Private ? undefined : this._containerAccessLevel;
            await containerClient.createIfNotExists({ access: publicAccessLevel });
        }
        return containerClient;
    }
}

export default MulterAzureStorage;
