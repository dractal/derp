Storage
=======

S3-compatible async file storage client. Access it via ``derp.storage``.

Config
------

.. code-block:: toml

   # derp.toml
   [storage]
   endpoint_url = "https://s3.amazonaws.com"
   access_key_id = "$AWS_ACCESS_KEY_ID"
   secret_access_key = "$AWS_SECRET_ACCESS_KEY"
   region = "us-east-1"
   # service_name = "s3"
   # use_ssl = true

Upload
------

.. code-block:: python

   await derp.storage.upload_file(
       bucket="assets",
       key="avatars/user.jpg",
       data=file_bytes,
       content_type="image/jpeg",
   )

Attach custom metadata:

.. code-block:: python

   await derp.storage.upload_file(
       bucket="assets",
       key="documents/report.pdf",
       data=pdf_bytes,
       content_type="application/pdf",
       metadata={"author": "alice", "version": "2"},
   )

Download
--------

.. code-block:: python

   data = await derp.storage.fetch_file(bucket="assets", key="avatars/user.jpg")

List Files
----------

.. code-block:: python

   keys = await derp.storage.list_files(bucket="assets", prefix="avatars/")
   # ["avatars/user1.jpg", "avatars/user2.jpg", ...]

Limit the number of results:

.. code-block:: python

   keys = await derp.storage.list_files(
       bucket="assets", prefix="avatars/", max_keys=10,
   )

Browse with folder-like structure using ``list_objects``:

.. code-block:: python

   result = await derp.storage.list_objects(bucket="assets", prefix="avatars/")
   # result["objects"]  -> list of {"key", "size", "last_modified"}
   # result["prefixes"] -> list of sub-prefixes (folders)

Delete
------

.. code-block:: python

   await derp.storage.delete_file(bucket="assets", key="avatars/user.jpg")

Check Existence
---------------

.. code-block:: python

   exists = await derp.storage.file_exists(bucket="assets", key="avatars/user.jpg")

Object Metadata
---------------

Fetch metadata without downloading the file body:

.. code-block:: python

   meta = await derp.storage.head_object(bucket="assets", key="avatars/user.jpg")
   # meta["content_type"]   -> "image/jpeg"
   # meta["content_length"] -> 102400
   # meta["last_modified"]  -> "2025-01-15T12:00:00+00:00"
   # meta["etag"]           -> "abc123"
   # meta["metadata"]       -> {"author": "alice"}

Get URL
-------

Build a URL from the configured endpoint:

.. code-block:: python

   url = derp.storage.get_url(bucket="assets", key="avatars/user.jpg")
   # "https://s3.amazonaws.com/assets/avatars/user.jpg"

Signed URLs
-----------

Generate time-limited URLs for direct client-side uploads and downloads
without proxying through your server.

**Download** -- give the frontend a temporary GET URL for a private object:

.. code-block:: python

   url = await derp.storage.signed_download_url(
       bucket="assets",
       key="avatars/user.jpg",
       expires_in=3600,  # 1 hour (default)
   )

**Upload** -- let the client PUT directly to S3:

.. code-block:: python

   url = await derp.storage.signed_upload_url(
       bucket="assets",
       key=f"uploads/{uuid4().hex}/{filename}",
       content_type="image/png",
       expires_in=300,
   )

Signing is a local crypto operation (no network call), so generating many URLs
in a loop is fine.

Batch Delete
------------

Delete multiple objects in a single request:

.. code-block:: python

   deleted = await derp.storage.delete_files(
       bucket="assets",
       keys=["tmp/a.txt", "tmp/b.txt", "tmp/c.txt"],
   )
   # deleted -> ["tmp/a.txt", "tmp/b.txt", "tmp/c.txt"]

Returns the list of keys that were successfully deleted.

Copy
----

Copy an object server-side (no download/re-upload):

.. code-block:: python

   # Same bucket
   await derp.storage.copy_file(
       src_bucket="assets",
       src_key="avatars/user.jpg",
       dst_key="avatars/user-backup.jpg",
   )

   # Across buckets
   await derp.storage.copy_file(
       src_bucket="uploads",
       src_key="tmp/photo.jpg",
       dst_bucket="assets",
       dst_key="avatars/user.jpg",
   )

``dst_bucket`` defaults to ``src_bucket`` when omitted.

List Buckets
------------

.. code-block:: python

   buckets = await derp.storage.list_buckets()
   # [{"name": "assets", "creation_date": "2025-01-01T00:00:00+00:00"}, ...]
