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

List Buckets
------------

.. code-block:: python

   buckets = await derp.storage.list_buckets()
   # [{"name": "assets", "creation_date": "2025-01-01T00:00:00+00:00"}, ...]
