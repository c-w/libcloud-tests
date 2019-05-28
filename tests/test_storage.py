import io
import json
import os
import random
import re
import string
import sys
import tempfile
import unittest

from libcloud.storage import providers, types


class StorageSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.provider = os.getenv("LIBCLOUD_PROVIDER", "azure_blobs")

        cls.kwargs = {
            "key": os.getenv("AZURE_STORAGE_ACCOUNT"),
            "secret": os.getenv("AZURE_STORAGE_KEY"),
        }

        cls.driver = providers.get_driver(cls.provider)(**cls.kwargs)

    def tearDown(self):
        for container in self.driver.list_containers():
            for obj in container.list_objects():
                obj.delete()
            container.delete()

    def test_containers(self):
        # make a new container
        container_name = _random_container_name()
        container = self.driver.create_container(container_name)
        self.assertEqual(container.name, container_name)
        container = self.driver.get_container(container_name)
        self.assertEqual(container.name, container_name)

        # check that an existing container can't be re-created
        with self.assertRaises(types.ContainerAlreadyExistsError):
            self.driver.create_container(container_name)

        # check that the new container can be listed
        containers = self.driver.list_containers()
        self.assertEqual([c.name for c in containers], [container_name])

        # delete the container
        self.driver.delete_container(container)

        # check that a deleted container can't be looked up
        with self.assertRaises(types.ContainerDoesNotExistError):
            self.driver.get_container(container_name)

        # check that the container is deleted
        containers = self.driver.list_containers()
        self.assertEqual([c.name for c in containers], [])

    def _test_objects(self, do_upload, do_download):
        content = b"some random content"
        blob_name = "testblob"
        container = self.driver.create_container(_random_container_name())

        # upload a file
        obj = do_upload(container, blob_name, content)
        self.assertEqual(obj.name, blob_name)
        obj = self.driver.get_object(container.name, blob_name)

        # check that the file can be listed
        blobs = self.driver.list_container_objects(container)
        self.assertEqual([blob.name for blob in blobs], [blob_name])

        # upload another file and check it's excluded in prefix listing
        do_upload(container, blob_name[::-1], content[::-1])
        blobs = self.driver.list_container_objects(container, ex_prefix=blob_name[0:3])
        self.assertEqual([blob.name for blob in blobs], [blob_name])

        # check that the file can be read back
        self.assertEqual(do_download(obj), content)

        # delete the file
        self.driver.delete_object(obj)

        # check that a missing file can't be deleted or looked up
        with self.assertRaises(types.ObjectDoesNotExistError):
            self.driver.delete_object(obj)
        with self.assertRaises(types.ObjectDoesNotExistError):
            self.driver.get_object(container.name, blob_name)

        # check that the file is deleted
        blobs = self.driver.list_container_objects(container)
        self.assertEqual([blob.name for blob in blobs], [blob_name[::-1]])

    def test_objects(self):
        def do_upload(container, blob_name, content):
            infile = self._create_tempfile(content=content)
            return self.driver.upload_object(infile, container, blob_name)

        def do_download(obj):
            outfile = self._create_tempfile()
            self.driver.download_object(obj, outfile, overwrite_existing=True)
            with open(outfile, "rb") as fobj:
                return fobj.read()

        self._test_objects(do_upload, do_download)

    def test_objects_stream_io(self):
        def do_upload(container, blob_name, content):
            content = io.BytesIO(content)
            return self.driver.upload_object_via_stream(content, container, blob_name)

        def do_download(obj):
            return _read_stream(self.driver.download_object_as_stream(obj))

        self._test_objects(do_upload, do_download)

    def test_objects_stream_iterable(self):
        def do_upload(container, blob_name, content):
            content = iter([content[i : i + 1] for i in range(len(content))])
            return self.driver.upload_object_via_stream(content, container, blob_name)

        def do_download(obj):
            return _read_stream(self.driver.download_object_as_stream(obj))

        self._test_objects(do_upload, do_download)

    def _create_tempfile(self, prefix="", content=b""):
        fobj, path = tempfile.mkstemp(prefix=prefix, text=False)
        os.write(fobj, content)
        os.close(fobj)
        self.addCleanup(os.remove, path)
        return path


def _random_container_name(prefix=""):
    max_length = 63
    suffix = _random_string(max_length)
    name = prefix + suffix
    name = re.sub("[^a-z0-9-]", "-", name)
    name = re.sub("-+", "-", name)
    return name[:max_length]


def _random_string(length, alphabet=string.ascii_lowercase + string.digits):
    return "".join(random.choice(alphabet) for _ in range(length))


def _read_stream(stream):
    buffer = io.BytesIO()
    buffer.writelines(stream)
    buffer.seek(0)
    return buffer.read()


class _storage_account:  # pylint:disable=invalid-name
    def __init__(self, client, location="eastus", name=None, template=None):
        self.client = client
        self.location = location
        self.resource_group_name = name or _random_string(length=23)
        self.template_path = template or os.path.splitext(__file__)[0] + ".arm.json"

    @property
    def template(self):
        with io.open(self.template_path, "r", encoding="utf-8") as fobj:
            return json.load(fobj)

    def __enter__(self):
        self.client.resource_groups.create_or_update(
            resource_group_name=self.resource_group_name,
            parameters={"location": self.location},
        )

        deployment = self.client.deployments.create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=os.path.basename(__file__),
            properties={"template": self.template, "mode": "incremental"},
        ).result()

        for key, value in deployment.properties.outputs.items():
            os.environ[key.upper()] = value["value"]

    def __exit__(self, *args, **kwargs):
        self.client.resource_groups.delete(
            resource_group_name=self.resource_group_name, polling=False
        )


def _main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode")

    storage_parser = subparsers.add_parser("azure-storage")
    storage_parser.add_argument("--password", required=True)
    storage_parser.add_argument("--tenant", required=True)
    storage_parser.add_argument("--username", required=True)
    storage_parser.add_argument("--subscription", required=True)

    args = parser.parse_args()

    if args.mode == "azure-storage":
        from azure.common.credentials import ServicePrincipalCredentials
        from azure.mgmt.resource import ResourceManagementClient

        credentials = ServicePrincipalCredentials(
            client_id=args.username, secret=args.password, tenant=args.tenant
        )
        client = ResourceManagementClient(credentials, args.subscription)

        with _storage_account(client):
            unittest.main(argv=[sys.argv[0]])


if __name__ == "__main__":
    _main()
