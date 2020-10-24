package com.salesforce.cantor.s3;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class S3Utils {
    private static final Logger logger = LoggerFactory.getLogger(S3Utils.class);

    // read objects in 4MB chunks
    private static final int streamingChunkSize = 4 * 1024 * 1024;

    /**
     * Get keys for objects in the bucket where the key matches a prefix.
     *
     * @param s3Client the client
     * @param bucketName bucket name
     * @param prefix the prefix to match keys against
     * @param start starting index
     * @param count number of keys to return
     * @return list of object keys in the bucket matching a prefix
     * @throws IOException any exception thrown from the s3 client
     */
    public static Collection<String> getKeys(final AmazonS3 s3Client,
                                             final String bucketName,
                                             final String prefix,
                                             final int start,
                                             final int count) throws IOException {
        final Set<String> keys = new HashSet<>();
        int index = 0;
        ObjectListing listing = null;
        try {
            do {
                if (listing == null) {
                    listing = s3Client.listObjects(bucketName, prefix);
                } else {
                    listing = s3Client.listNextBatchOfObjects(listing);
                }
                final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
                // skip sections that the start index wouldn't include
                if ((objectSummaries.size() - 1) + index < start) {
                    index += objectSummaries.size();
                    logger.debug("skipping {} objects to index={}", objectSummaries.size(), index);
                    listing = s3Client.listNextBatchOfObjects(listing);
                    continue;
                }
                for (final S3ObjectSummary summary : objectSummaries) {
                    if (start > index++) {
                        continue;
                    }
                    keys.add(summary.getKey());
                    if (keys.size() == count) {
                        logger.debug("retrieved {}/{} keys, returning early", keys.size(), count);
                        return keys;
                    }
                }
                logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
            } while (listing.isTruncated());
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }

        return keys;
    }

    /**
     * Get content of an object.
     *
     * @param s3Client the client
     * @param bucketName the bucket name
     * @param key object key
     * @return content of the object as bytes, null if not found
     * @throws IOException any exception thrown from the s3 client
     */
    public static byte[] getObjectBytes(final AmazonS3 s3Client,
                                        final String bucketName,
                                        final String key) throws IOException {
        if (!s3Client.doesObjectExist(bucketName, key)) {
            return null;
        }
        final S3Object s3Object = s3Client.getObject(bucketName, key);
        final ByteArrayOutputStream buffer;
        try (final InputStream inputStream = s3Object.getObjectContent()) {
            buffer = new ByteArrayOutputStream();
            final byte[] data = new byte[streamingChunkSize];
            int read;
            while ((read = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, read);
            }
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    /**
     * Get input stream to read content of an object.
     *
     * @param s3Client the client
     * @param bucketName the bucket name
     * @param key object key
     * @return input stream to read content of the object, null if not found
     * @throws IOException any exception thrown from the s3 client
     */
    public static InputStream getObjectStream(final AmazonS3 s3Client,
                                              final String bucketName,
                                              final String key) throws IOException {
        try {
            final S3Object object = s3Client.getObject(bucketName, key);
            return object != null ? object.getObjectContent() : null;
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Store an object.
     *
     * @param s3Client the client
     * @param bucketName bucket name
     * @param key object key
     * @param content input stream to read the content of the object from
     * @param metadata metadata to attach to object
     * @throws IOException any exception thrown from the s3 client
     */
    public static void putObject(final AmazonS3 s3Client,
                                 final String bucketName,
                                 final String key,
                                 final InputStream content,
                                 final ObjectMetadata metadata) throws IOException {
        try {
            s3Client.putObject(bucketName, key, content, metadata);
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Delete an object from S3.
     *
     * @param s3Client the client
     * @param bucketName bucket name
     * @param key object key to be deleted
     * @return true if object is found and successfully deleted, false otherwise
     * @throws IOException any exception thrown from the s3 client
     */
    public static boolean deleteObject(final AmazonS3 s3Client, final String bucketName, final String key) throws IOException {
        try {
            if (!s3Client.doesObjectExist(bucketName, key)) {
                return false;
            }

            s3Client.deleteObject(bucketName, key);
            return true;
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Delete all objects in a bucket that their key matches a prefix.
     *
     * @param s3Client the client
     * @param bucketName bucket name
     * @param prefix the prefix to match against all objects in the bucket to be deleted
     * @throws IOException any exception thrown from the s3 client
     */
    public static void deleteObjects(final AmazonS3 s3Client,
                                     final String bucketName,
                                     final String prefix) throws IOException {
        logger.info("dropping objects with prefix '{}' from bucket '{}'", prefix, bucketName);
        // delete all objects
        try {
            ObjectListing objectListing = s3Client.listObjects(bucketName, prefix);
            while (true) {
                for (final S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    s3Client.deleteObject(bucketName, summary.getKey());
                }
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get number of objects in the bucket where their keys match a prefix.
     *
     * @param s3Client the client
     * @param bucketName bucket name
     * @param prefix the prefix to match against object keys to include in count
     * @return number of objects in the bucket with keys matching the given prefix
     * @throws IOException any exception thrown from the s3 client
     */
    public static int getSize(final AmazonS3 s3Client, final String bucketName, final String prefix) throws IOException {
        logger.info("dropping objects with prefix '{}' from bucket '{}'", prefix, bucketName);
        int totalSize = 0;
        ObjectListing listing = null;
        try {
            do {
                if (listing == null) {
                    listing = s3Client.listObjects(bucketName, prefix);
                } else {
                    listing = s3Client.listNextBatchOfObjects(listing);
                }
                totalSize += listing.getObjectSummaries().size();
            } while (listing.isTruncated());
        } catch (SdkBaseException e) {
            throw new IOException(e);
        }
        return totalSize;
    }
}
