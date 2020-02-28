import base64
import io
import json
import os
import random
import re
import string
import tempfile
import time
import unittest

import requests
from libcloud.storage import providers, types

MB = 1024 * 1024


class SmokeStorageTest(unittest.TestCase):
    class Config:
        provider = None
        account = None
        secret = None

    def setUp(self):
        for required in "provider", "account", "secret":
            value = getattr(self.Config, required, None)
            if value is None:
                raise unittest.SkipTest("config {} not set".format(required))

        kwargs = {"key": self.Config.account, "secret": self.Config.secret}

        for optional in "host", "port", "secure":
            value = getattr(self.Config, optional, None)
            if value is not None:
                kwargs[optional] = value

        self.driver = providers.get_driver(self.Config.provider)(**kwargs)

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

    def _test_objects(self, do_upload, do_download, size=1 * MB):
        content = os.urandom(size)
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

    def test_objects(self, size=1 * MB):
        def do_upload(container, blob_name, content):
            infile = self._create_tempfile(content=content)
            return self.driver.upload_object(infile, container, blob_name)

        def do_download(obj):
            outfile = self._create_tempfile()
            self.driver.download_object(obj, outfile, overwrite_existing=True)
            with open(outfile, "rb") as fobj:
                return fobj.read()

        self._test_objects(do_upload, do_download, size)

    def test_objects_range_downloads(self):
        blob_name = "testblob-range"
        content = b"0123456789"
        container = self.driver.create_container(_random_container_name())

        infile = self._create_tempfile(content=content)
        obj = self.driver.upload_object(infile, container, blob_name)
        self.assertEqual(obj.name, blob_name)
        self.assertEqual(obj.size, len(content))

        obj = self.driver.get_object(container.name, blob_name)
        self.assertEqual(obj.name, blob_name)
        self.assertEqual(obj.size, len(content))

        values = [
            {
                "start_bytes": 0,
                "end_bytes": 0
            },
            {
                "start_bytes": 1,
                "end_bytes": 5
            },
            {
                "start_bytes": 5,
                "end_bytes": None
            },
            {
                "start_bytes": 5,
                "end_bytes": len(content)
            }
        ]

        for value in values:
            # 1. download_object_range
            start_bytes = value["start_bytes"]
            end_bytes = value["end_bytes"]
            outfile = self._create_tempfile()

            result = self.driver.download_object_range(obj, outfile,
                                              start_bytes=start_bytes,
                                              end_bytes=end_bytes,
                                              overwrite_existing=True)
            self.assertTrue(result)

            with open(outfile, "rb") as fobj:
                downloaded_content = fobj.read()

            if end_bytes is not None:
                expected_content = content[start_bytes:end_bytes + 1]
            else:
                expected_content = content[start_bytes]

            self.assertEqual(downloaded_content, expected_content)

            # 2. download_object_range_as_stream
            downloaded_content = _read_stream(self.driver.download_object_as_stream(obj))
            self.assertEqual(downloaded_content, expected_content)

    @unittest.skipUnless(os.getenv("LARGE_FILE_SIZE_MB"), "config not set")
    def test_objects_large(self):
        size = int(float(os.environ["LARGE_FILE_SIZE_MB"]) * MB)
        self.test_objects(size)

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

    def test_cdn_url(self):
        content = os.urandom(MB // 100)
        container = self.driver.create_container(_random_container_name())
        obj = self.driver.upload_object_via_stream(iter(content), container, "cdn")

        response = requests.get(self.driver.get_object_cdn_url(obj))
        response.raise_for_status()

        self.assertEqual(response.content, content)

    def _create_tempfile(self, prefix="", content=b""):
        fobj, path = tempfile.mkstemp(prefix=prefix, text=False)
        os.write(fobj, content)
        os.close(fobj)
        self.addCleanup(os.remove, path)
        return path


class AzureStorageTest(SmokeStorageTest):
    class Config:
        username = None
        password = None
        tenant = None
        subscription = None
        location = "eastus"
        template_file = os.path.splitext(__file__)[0] + ".arm.json"
        kind = "StorageV2"

    client = None
    resource_group_name = None

    @classmethod
    def setUpClass(cls):
        try:
            from azure.common.credentials import ServicePrincipalCredentials
            from azure.mgmt.resource import ResourceManagementClient
        except ImportError:
            raise unittest.SkipTest("missing azure-mgmt-resource library")

        cls.client = ResourceManagementClient(
            credentials=ServicePrincipalCredentials(
                client_id=cls.Config.username,
                secret=cls.Config.password,
                tenant=cls.Config.tenant,
            ),
            subscription_id=cls.Config.subscription,
        )

        cls.resource_group_name = "libcloudtest" + _random_string(length=23)

        cls.client.resource_groups.create_or_update(
            resource_group_name=cls.resource_group_name,
            parameters={"location": cls.Config.location},
        )

        with io.open(cls.Config.template_file, encoding="utf-8") as fobj:
            template = json.load(fobj)

        deployment = cls.client.deployments.create_or_update(
            resource_group_name=cls.resource_group_name,
            deployment_name=os.path.basename(__file__),
            properties={
                "template": template,
                "mode": "incremental",
                "parameters": {"storageAccountKind": {"value": cls.Config.kind}},
            },
        ).result()

        for key, value in deployment.properties.outputs.items():
            setattr(cls.Config, key.lower(), value["value"])

        cls.Config.provider = "azure_blobs"

    @classmethod
    def tearDownClass(cls):
        while True:
            groups = [
                group
                for group in cls.client.resource_groups.list()
                if group.name.startswith(cls.resource_group_name)
            ]

            if not groups:
                break

            for group in groups:
                if group.properties.provisioning_state != "Deleting":
                    try:
                        cls.client.resource_groups.delete(
                            resource_group_name=group.name, polling=False
                        )
                    except Exception:  # pylint: disable=broad-except
                        pass

            time.sleep(3)


class AzuriteStorageTest(SmokeStorageTest):
    class Config:
        port = 10000
        version = "latest"

    client = None
    container = None
    image = "arafato/azurite"
    has_sas_support = False

    @classmethod
    def setUpClass(cls):
        cls.client = _new_docker_client()

        cls.container = cls.client.containers.run(
            "{}:{}".format(cls.image, cls.Config.version),
            detach=True,
            auto_remove=True,
            ports={cls.Config.port: 10000},
            environment={"executable": "blob"},
        )

        cls.Config.account = "devstoreaccount1"
        cls.Config.secret = (
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uS"
            "RZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        )
        cls.Config.host = "localhost"
        cls.Config.secure = False
        cls.Config.provider = "azure_blobs"

        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        _kill_and_log(cls.container)

    def test_cdn_url(self):
        if not self.has_sas_support:
            self.skipTest("Storage backend has no account SAS support")


class AzuriteV3StorageTest(AzuriteStorageTest):
    image = "mcr.microsoft.com/azure-storage/azurite"
    has_sas_support = True


class IotedgeStorageTest(SmokeStorageTest):
    class Config:
        port = 11002
        version = "latest"

    client = None
    container = None

    @classmethod
    def setUpClass(cls):
        cls.client = _new_docker_client()

        account = _random_string(10)
        key = base64.b64encode(_random_string(20).encode("ascii")).decode("ascii")

        cls.container = cls.client.containers.run(
            "mcr.microsoft.com/azure-blob-storage:{}".format(cls.Config.version),
            detach=True,
            auto_remove=True,
            ports={cls.Config.port: 11002},
            environment={
                "LOCAL_STORAGE_ACCOUNT_NAME": account,
                "LOCAL_STORAGE_ACCOUNT_KEY": key,
            },
        )

        cls.Config.account = account
        cls.Config.secret = key
        cls.Config.host = "localhost"
        cls.Config.secure = False
        cls.Config.provider = "azure_blobs"

        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        _kill_and_log(cls.container)


def _new_docker_client():
    try:
        import docker
    except ImportError:
        raise unittest.SkipTest("missing docker library")

    return docker.from_env()


def _kill_and_log(container):
    for line in container.logs().splitlines():
        print(line)
    container.kill()


def _random_container_name(prefix="test"):
    max_length = 63
    suffix = _random_string(max_length)
    name = prefix + suffix
    name = re.sub("[^a-z0-9-]", "-", name)
    name = re.sub("-+", "-", name)
    name = name[:max_length]
    name = name.lower()
    return name


def _random_string(length, alphabet=string.ascii_lowercase + string.digits):
    return "".join(random.choice(alphabet) for _ in range(length))


def _read_stream(stream):
    buffer = io.BytesIO()
    buffer.writelines(stream)
    buffer.seek(0)
    return buffer.read()


def _cli(module_name, strip_suffix=""):
    import argparse
    import inspect
    import sys

    module = sys.modules[module_name]

    testcases = {
        class_name.replace(strip_suffix, "").lower(): test_class
        for class_name, test_class in inspect.getmembers(module, inspect.isclass)
    }

    testcase_arg = "mode"

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest=testcase_arg)

    for test_name, test_class in testcases.items():
        test_parser = subparsers.add_parser(test_name)
        for arg_name, arg_value in vars(test_class.Config).items():
            if not arg_name.startswith("_"):
                kwargs = {}
                if arg_value is None:
                    kwargs["required"] = True
                else:
                    kwargs["type"] = type(arg_value)
                    kwargs["default"] = arg_value
                test_parser.add_argument("--{}".format(arg_name), **kwargs)

    args = parser.parse_args()

    testcase = testcases[getattr(args, testcase_arg)]
    for arg_name, arg_value in vars(args).items():
        if not arg_name.startswith("_") and arg_name != testcase_arg:
            setattr(testcase.Config, arg_name, arg_value)

    unittest.main(argv=[sys.argv[0], testcase.__name__])


if __name__ == "__main__":
    _cli(__name__, "StorageTest")
