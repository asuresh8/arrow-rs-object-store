// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cloud Multipart Upload
//!
//! This crate provides an asynchronous interface for multipart file uploads to
//! cloud storage services. It's designed to offer efficient, non-blocking operations,
//! especially useful when dealing with large files or high-throughput systems.

use async_trait::async_trait;

use crate::path::Path;
use crate::{
    Error, Extensions, MultipartId, PutMode, PutMultipartOptions, PutPayload, PutResult, Result,
};

/// Options for [`MultipartStore::complete_multipart_opts`]
#[derive(Debug, Clone, Default)]
pub struct CompleteMultipartOptions {
    /// Configure the [`PutMode`] applied when the upload completes
    ///
    /// [`PutMode::Create`] makes completion fail with [`Error::AlreadyExists`]
    /// if an object already exists at the path. The upload is then left in
    /// place, so it can still be aborted with [`MultipartStore::abort_multipart`].
    ///
    /// Implementations that don't support conditional completion return
    /// [`Error::NotSupported`] or [`Error::NotImplemented`] for any mode other
    /// than [`PutMode::Overwrite`].
    pub mode: PutMode,
    /// Implementation-specific extensions. Intended for use by [`MultipartStore`]
    /// implementations that need to pass context-specific information (like tracing spans)
    /// via trait methods.
    ///
    /// These extensions are ignored entirely by backends offered through this crate.
    pub extensions: Extensions,
}

/// Represents a part of a file that has been successfully uploaded in a multipart upload process.
#[derive(Debug, Clone)]
pub struct PartId {
    /// Id of this part
    pub content_id: String,
}

/// A low-level interface for interacting with multipart upload APIs
///
/// Most use-cases should prefer [`ObjectStore::put_multipart_opts`] as this is supported by more
/// backends, including [`LocalFileSystem`], and automatically handles uploading fixed
/// size parts of sufficient size in parallel
///
/// See [`AmazonS3::create_multipart`] for a full example of this API.
///
/// [`ObjectStore::put_multipart_opts`]: crate::ObjectStore::put_multipart_opts
/// [`LocalFileSystem`]: crate::local::LocalFileSystem
/// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
#[async_trait]
pub trait MultipartStore: Send + Sync + 'static {
    /// Creates a new multipart upload, returning the [`MultipartId`]
    ///
    /// See [`AmazonS3::create_multipart`] for a full example of this API.
    ///
    /// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId>;

    /// Creates a new multipart upload with the given options, returning the [`MultipartId`]
    ///
    /// This allows callers using the low-level multipart API to provide object attributes,
    /// tags, or implementation-specific extensions when initiating the upload.
    ///
    /// See [`AmazonS3::create_multipart`] for a full example of this API.
    ///
    /// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
    async fn create_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let PutMultipartOptions {
            mode,
            tags,
            attributes,
            extensions: _,
        } = opts;

        if mode != PutMode::Overwrite || !tags.is_empty() || !attributes.is_empty() {
            return Err(Error::NotSupported {
                source: "create_multipart_opts with non-default options".into(),
            });
        }

        self.create_multipart(path).await
    }

    /// Uploads a new part with index `part_idx`
    ///
    /// `part_idx` should be an integer in the range `0..N` where `N` is the number of
    /// parts in the upload. Parts may be uploaded concurrently and in any order.
    ///
    /// Most stores require that all parts excluding the last are at least 5 MiB, and some
    /// further require that all parts excluding the last be the same size, e.g. [R2].
    /// [`WriteMultipart`] performs writes in fixed size blocks of 5 MiB, and clients wanting
    /// to maximise compatibility should look to do likewise.
    ///
    /// [R2]: https://developers.cloudflare.com/r2/objects/multipart-objects/#limitations
    /// [`WriteMultipart`]: crate::upload::WriteMultipart
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId>;

    /// Completes a multipart upload
    ///
    /// The `i`'th value of `parts` must be a [`PartId`] returned by a call to [`Self::put_part`]
    /// with a `part_idx` of `i`, and the same `path` and `id` as provided to this method. Calling
    /// this method with out of sequence or repeated [`PartId`], or [`PartId`] returned for other
    /// values of `path` or `id`, will result in implementation-defined behaviour
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult>;

    /// Completes a multipart upload with the given options
    ///
    /// This is [`Self::complete_multipart`] with a [`PutMode`] applied at completion,
    /// which is where a store such as S3 evaluates the condition. With [`PutMode::Create`],
    /// it fails with [`Error::AlreadyExists`] if an object already exists at `path`.
    ///
    /// The default implementation supports only [`PutMode::Overwrite`], and returns
    /// [`Error::NotSupported`] for any other mode.
    async fn complete_multipart_opts(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
        opts: CompleteMultipartOptions,
    ) -> Result<PutResult> {
        let CompleteMultipartOptions {
            mode,
            extensions: _,
        } = opts;

        if mode != PutMode::Overwrite {
            return Err(Error::NotSupported {
                source: "complete_multipart_opts with a mode other than PutMode::Overwrite".into(),
            });
        }

        self.complete_multipart(path, id, parts).await
    }

    /// Aborts a multipart upload
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Extensions;

    struct TestMultipartStore;

    #[async_trait]
    impl MultipartStore for TestMultipartStore {
        async fn create_multipart(&self, _path: &Path) -> Result<MultipartId> {
            Ok("test".into())
        }

        async fn put_part(
            &self,
            _path: &Path,
            _id: &MultipartId,
            _part_idx: usize,
            _data: PutPayload,
        ) -> Result<PartId> {
            unreachable!()
        }

        async fn complete_multipart(
            &self,
            _path: &Path,
            _id: &MultipartId,
            _parts: Vec<PartId>,
        ) -> Result<PutResult> {
            unreachable!()
        }

        async fn abort_multipart(&self, _path: &Path, _id: &MultipartId) -> Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn default_create_multipart_opts_ignores_extensions() {
        let mut extensions = Extensions::new();
        extensions.insert("extension");
        let opts = PutMultipartOptions {
            extensions,
            ..Default::default()
        };

        let id = TestMultipartStore
            .create_multipart_opts(&Path::from("test"), opts)
            .await
            .unwrap();

        assert_eq!(id, "test");
    }

    #[tokio::test]
    async fn default_complete_multipart_opts_rejects_create() {
        let opts = CompleteMultipartOptions {
            mode: PutMode::Create,
            ..Default::default()
        };

        let err = TestMultipartStore
            .complete_multipart_opts(&Path::from("test"), &"test".into(), vec![], opts)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotSupported { .. }), "{err}");
    }
}
