import base64
import io
import json
import os
import random
import re
import string
import sys
import tempfile
import time
import unittest

from libcloud.storage import providers, types


class StorageSmokeTest(unittest.TestCase):
    account = None
    secret = None

    def setUp(self):
        self.provider = os.getenv("LIBCLOUD_PROVIDER", "azure_blobs")

        self.kwargs = {
            "key": self.account or os.getenv("AZURE_STORAGE_ACCOUNT"),
            "secret": self.secret or os.getenv("AZURE_STORAGE_KEY"),
        }

        if not self.kwargs["key"] or not self.kwargs["secret"]:
            raise unittest.SkipTest("key and/or secret not set")

        try:
            self.kwargs["host"] = os.environ["AZURE_STORAGE_HOST"]
        except KeyError:
            pass

        try:
            self.kwargs["port"] = int(os.environ["AZURE_STORAGE_PORT"])
        except KeyError:
            pass

        try:
            self.kwargs["secure"] = os.environ["AZURE_STORAGE_SECURE"] != "false"
        except KeyError:
            pass

        self.driver = providers.get_driver(self.provider)(**self.kwargs)

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


class AzureStorageTest(StorageSmokeTest):
    username = None
    password = None
    tenant = None
    subscription = None
    client = None
    resource_group_name = None
    location = "eastus"
    template_file = os.path.splitext(__file__)[0] + ".arm.json"

    @classmethod
    def setUpClass(cls):
        try:
            from azure.common.credentials import ServicePrincipalCredentials
            from azure.mgmt.resource import ResourceManagementClient
        except ImportError:
            raise unittest.SkipTest("missing azure-mgmt-resource library")

        cls.client = ResourceManagementClient(
            credentials=ServicePrincipalCredentials(
                client_id=cls.username, secret=cls.password, tenant=cls.tenant
            ),
            subscription_id=cls.subscription,
        )

        cls.resource_group_name = _random_string(length=23)

        cls.client.resource_groups.create_or_update(
            resource_group_name=cls.resource_group_name,
            parameters={"location": cls.location},
        )

        with io.open(cls.template_file, encoding="utf-8") as fobj:
            template = json.load(fobj)

        deployment = cls.client.deployments.create_or_update(
            resource_group_name=cls.resource_group_name,
            deployment_name=os.path.basename(__file__),
            properties={"template": template, "mode": "incremental"},
        ).result()

        for key, value in deployment.properties.outputs.items():
            os.environ[key.upper()] = value["value"]

    @classmethod
    def tearDownClass(cls):
        cls.client.resource_groups.delete(
            resource_group_name=cls.resource_group_name, polling=False
        )


class AzuriteStorageTest(StorageSmokeTest):
    client = None
    container = None
    port = 10000
    version = "latest"

    @classmethod
    def setUpClass(cls):
        try:
            import docker
        except ImportError:
            raise unittest.SkipTest("missing docker library")

        cls.client = docker.from_env()

        cls.container = cls.client.containers.run(
            "arafato/azurite:{}".format(cls.version),
            detach=True,
            auto_remove=True,
            ports={cls.port: 10000},
            environment={"executable": "blob"},
        )

        os.environ["AZURE_STORAGE_ACCOUNT"] = "devstoreaccount1"
        os.environ["AZURE_STORAGE_KEY"] = (
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uS"
            "RZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        )
        os.environ["AZURE_STORAGE_PORT"] = str(cls.port)
        os.environ["AZURE_STORAGE_HOST"] = "localhost"
        os.environ["AZURE_STORAGE_SECURE"] = "false"

        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        _kill_and_log(cls.container)


class IotedgeStorageTests(StorageSmokeTest):
    client = None
    container = None
    port = 11002
    version = "latest"

    @classmethod
    def setUpClass(cls):
        try:
            import docker
        except ImportError:
            raise unittest.SkipTest("missing docker library")

        cls.client = docker.from_env()

        account = _random_string(10)
        key = base64.b64encode(_random_string(20).encode("ascii")).decode("ascii")

        cls.container = cls.client.containers.run(
            "mcr.microsoft.com/azure-blob-storage:{}".format(cls.version),
            detach=True,
            auto_remove=True,
            ports={cls.port: 11002},
            environment={
                "LOCAL_STORAGE_ACCOUNT_NAME": account,
                "LOCAL_STORAGE_ACCOUNT_KEY": key,
            },
        )

        os.environ["AZURE_STORAGE_ACCOUNT"] = account
        os.environ["AZURE_STORAGE_KEY"] = key
        os.environ["AZURE_STORAGE_PORT"] = str(cls.port)
        os.environ["AZURE_STORAGE_HOST"] = "localhost"
        os.environ["AZURE_STORAGE_SECURE"] = "false"

        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        _kill_and_log(cls.container)


def _kill_and_log(container):
    for line in container.logs().splitlines():
        print(line)
    container.kill()


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


def _main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode")

    storage_parser = subparsers.add_parser("azure-storage")
    storage_parser.add_argument("--account", required=True)
    storage_parser.add_argument("--secret", required=True)

    new_storage_parser = subparsers.add_parser("new-azure-storage")
    new_storage_parser.add_argument("--password", required=True)
    new_storage_parser.add_argument("--tenant", required=True)
    new_storage_parser.add_argument("--username", required=True)
    new_storage_parser.add_argument("--subscription", required=True)
    new_storage_parser.add_argument("--location", default="eastus")

    azurite_parser = subparsers.add_parser("azurite")
    azurite_parser.add_argument("--port", type=int, default=10000)
    azurite_parser.add_argument("--version", default="latest")

    iotedge_parser = subparsers.add_parser("iotedge")
    iotedge_parser.add_argument("--port", type=int, default=11002)
    iotedge_parser.add_argument("--version", default="latest")

    args = parser.parse_args()

    if args.mode == "azure-storage":
        StorageSmokeTest.account = args.account
        StorageSmokeTest.secret = args.secret

        testcase = StorageSmokeTest

    elif args.mode == "new-azure-storage":
        AzureStorageTest.username = args.username
        AzureStorageTest.password = args.password
        AzureStorageTest.tenant = args.tenant
        AzureStorageTest.subscription = args.subscription
        AzureStorageTest.location = args.location

        testcase = AzureStorageTest

    elif args.mode == "azurite":
        AzuriteStorageTest.port = args.port
        AzuriteStorageTest.version = args.version

        testcase = AzuriteStorageTest

    elif args.mode == "iotedge":
        IotedgeStorageTests.port = args.port
        IotedgeStorageTests.version = args.version

        testcase = IotedgeStorageTests

    else:
        raise NotImplementedError

    unittest.main(argv=[sys.argv[0], testcase.__name__])


if __name__ == "__main__":
    _main()
