/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.archive.file.AbstractBaseArchiverOnFile;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ObjectsArchiverOnS3 extends AbstractBaseArchiverOnFile implements ObjectsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsArchiverOnS3.class);
    private static final String archiveFilename = "archive-objects-%s-%d";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public ObjectsArchiverOnS3(final Cantor s3Cantor, final String baseDirectory) {
        super(baseDirectory);
    }

    @Override
    public void archive(final Objects objects, final String namespace) throws IOException {
        // not implemented yet
    }

    @Override
    public void restore(final Objects objects, final String namespace) throws IOException {
        // not implemented yet
    }
}
