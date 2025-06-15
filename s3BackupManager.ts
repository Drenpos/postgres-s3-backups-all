import { S3Client, ListObjectsV2Command, DeleteObjectCommand } from "@aws-sdk/client-s3";

// ...existing imports...

export class S3BackupManager {
    // ...existing code...

    async deleteOldBackups(bucketName: string) {
        const s3 = new S3Client({ /* ...config... */ });
        const fourDaysAgo = new Date(Date.now() - 4 * 24 * 60 * 60 * 1000);

        let continuationToken: string | undefined = undefined;
        do {
            const listParams = {
                Bucket: bucketName,
                ContinuationToken: continuationToken,
            };
            const listResult = await s3.send(new ListObjectsV2Command(listParams));
            const oldObjects = (listResult.Contents || []).filter(obj =>
                obj.LastModified && obj.LastModified < fourDaysAgo
            );

            for (const obj of oldObjects) {
                if (obj.Key) {
                    await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: obj.Key }));
                }
            }

            continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
        } while (continuationToken);
    }

    // ...existing code...
}