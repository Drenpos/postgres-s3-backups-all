import {exec, execSync} from "child_process";
import {
     S3Client,
     S3ClientConfig,
     PutObjectCommandInput,
     ListObjectsV2Command,
     DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import {Upload} from "@aws-sdk/lib-storage";
import {createReadStream, unlink, statSync} from "fs";
import {filesize} from "filesize";
import path from "path";
import os from "os";

import {env} from "./env.js";
import {createMD5} from "./util.js";

const uploadToS3 = async ({name, path}: {name: string; path: string}) => {
     console.log("Uploading backup to S3...");

     const bucket = env.AWS_S3_BUCKET;

     const clientOptions: S3ClientConfig = {
          region: env.AWS_S3_REGION,
          forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE,
     };

     if (env.AWS_S3_ENDPOINT) {
          console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);

          clientOptions.endpoint = env.AWS_S3_ENDPOINT;
     }

     if (env.BUCKET_SUBFOLDER) {
          name = env.BUCKET_SUBFOLDER + "/" + name;
     }

     let params: PutObjectCommandInput = {
          Bucket: bucket,
          Key: name,
          Body: createReadStream(path),
     };

     if (env.SUPPORT_OBJECT_LOCK) {
          console.log("MD5 hashing file...");

          const md5Hash = await createMD5(path);

          console.log("Done hashing file");

          params.ContentMD5 = Buffer.from(md5Hash, "hex").toString("base64");
     }

     const client = new S3Client(clientOptions);

     await new Upload({
          client,
          params: params,
     }).done();

     console.log("Backup uploaded to S3...");
};
const dumpToFile = async (dbUrl: string, filePath: string) => {
     console.log(`Dumping ${dbUrl} to file…`);

     await new Promise((resolve, reject) => {
          exec(
               `pg_dump --dbname=${dbUrl} --format=tar ${env.BACKUP_OPTIONS} | gzip > ${filePath}`,
               (error, stdout, stderr) => {
                    if (error) {
                         reject({error: error, stderr: stderr.trimEnd()});
                         return;
                    }

                    // check if archive is valid and contains data
                    const isValidArchive =
                         execSync(`gzip -cd ${filePath} | head -c1`).length == 1 ? true : false;
                    if (isValidArchive == false) {
                         reject({
                              error: "Backup archive file is invalid or empty; check for errors above",
                         });
                         return;
                    }

                    // not all text in stderr will be a critical error, print the error / warning
                    if (stderr != "") {
                         console.log({stderr: stderr.trimEnd()});
                    }

                    console.log("Backup archive file is valid");
                    console.log("Backup filesize:", filesize(statSync(filePath).size));

                    // if stderr contains text, let the user know that it was potently just a warning message
                    if (stderr != "") {
                         console.log(
                              `Potential warnings detected; Please ensure the backup file "${path.basename(
                                   filePath,
                              )}" contains all needed data`,
                         );
                    }

                    resolve(undefined);
               },
          );
     });
     console.log("DB dumped to file...");
};

const dumpToFileOld = async (filePath: string) => {
     console.log("Dumping DB to file...");

     await new Promise((resolve, reject) => {
          exec(
               `pg_dump --dbname=${env.BACKUP_DATABASE_URL} --format=tar ${env.BACKUP_OPTIONS} | gzip > ${filePath}`,
               (error, stdout, stderr) => {
                    if (error) {
                         reject({error: error, stderr: stderr.trimEnd()});
                         return;
                    }

                    // check if archive is valid and contains data
                    const isValidArchive =
                         execSync(`gzip -cd ${filePath} | head -c1`).length == 1 ? true : false;
                    if (isValidArchive == false) {
                         reject({
                              error: "Backup archive file is invalid or empty; check for errors above",
                         });
                         return;
                    }

                    // not all text in stderr will be a critical error, print the error / warning
                    if (stderr != "") {
                         console.log({stderr: stderr.trimEnd()});
                    }

                    console.log("Backup archive file is valid");
                    console.log("Backup filesize:", filesize(statSync(filePath).size));

                    // if stderr contains text, let the user know that it was potently just a warning message
                    if (stderr != "") {
                         console.log(
                              `Potential warnings detected; Please ensure the backup file "${path.basename(
                                   filePath,
                              )}" contains all needed data`,
                         );
                    }

                    resolve(undefined);
               },
          );
     });

     console.log("DB dumped to file...");
};

const deleteFile = async (path: string) => {
     console.log("Deleting file...");
     await new Promise((resolve, reject) => {
          unlink(path, (err) => {
               reject({error: err});
               return;
          });
          resolve(undefined);
     });
};

/**
 * Devuelve un array con todos los nombres de bases de datos
 * que no son plantillas ni 'postgres'.
 */
const listDatabases = (clusterUrl: string): Promise<string[]> => {
     return new Promise((resolve, reject) => {
          const query =
               `SELECT datname FROM pg_database ` +
               `WHERE datistemplate = false AND datname NOT IN ('postgres');`;

          // -A  = unaligned   -t = tuples only  → devuelve solo los nombres
          exec(`psql "${clusterUrl}" -At -c "${query}"`, (error, stdout, stderr) => {
               if (error) {
                    return reject(error); // cualquier error crítico
               }

               if (stderr.trim() !== "") {
                    console.log({stderr: stderr.trim()}); // avisos o warnings
               }

               const dbs = stdout.trim().split("\n").filter(Boolean);
               resolve(dbs);
          });
     });
};

const deleteOldBackups = async (bucketName: string) => {
     const s3 = new S3Client({
          /* ...config... */
     });
     const fourDaysAgo = new Date(Date.now() - 4 * 24 * 60 * 60 * 1000);

     let continuationToken: string | undefined = undefined;
     do {
          const listParams: any = {
               Bucket: bucketName,
               ContinuationToken: continuationToken,
          };
          const listResult: any = await s3.send(new ListObjectsV2Command(listParams));
          const oldObjects = (listResult.Contents || []).filter(
               (obj: any) => obj.LastModified && obj.LastModified < fourDaysAgo,
          );

          for (const obj of oldObjects) {
               if (obj.Key) {
                    await s3.send(new DeleteObjectCommand({Bucket: bucketName, Key: obj.Key}));
               }
          }

          continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
     } while (continuationToken);
};

export const backup = async () => {
     console.log("Initiating DB backup...");

     const dbs = await listDatabases(env.BACKUP_DATABASE_URL);
     await deleteOldBackups(env.AWS_S3_BUCKET);
     console.log("Encontradas bases:", dbs.join(", "));

     const date = new Date().toISOString();
     const timestamp = date.replace(/[:.]+/g, "-");

     for (const db of dbs) {
          const filename = `${db}_${env.BACKUP_FILE_PREFIX}-${timestamp}.tar.gz`;
          const filepath = path.join(os.tmpdir(), filename);

          // Construye una URL igual que BACKUP_DATABASE_URL pero con la DB actual
          const dbUrl = env.BACKUP_DATABASE_URL.replace(/\/[^/]+$/, `/${db}`);

          await dumpToFile(dbUrl, filepath);
          await uploadToS3({name: filename, path: filepath});
          await deleteFile(filepath);

          console.log(`✔ Backup de "${db}" completado`);
     }
     console.log("DB backup complete...");
};

export const backupOld = async () => {
     console.log("Initiating DB backup...");

     const date = new Date().toISOString();
     const timestamp = date.replace(/[:.]+/g, "-");
     const filename = `${env.BACKUP_FILE_PREFIX}-${timestamp}.tar.gz`;
     const filepath = path.join(os.tmpdir(), filename);

     await dumpToFileOld(filepath);
     await uploadToS3({name: filename, path: filepath});
     await deleteFile(filepath);

     console.log("DB backup complete...");
};
