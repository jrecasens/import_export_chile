try:
  import config
except ImportError:
  from . import config

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.fileshare import ShareFileClient
import os
import pathlib

class AzureBlogStorage:
    """Connector to interact with Azure Blog Storage """

    def __init__(self, conn_str):
        # Configure Tolveet Logger
        TL = config.TolveetLogger()
        self.logger = TL.get_tolveet_logger()
        self.service_client = self._create_service_client(conn_str)

    def _create_service_client(self, conn_str):
        """Create the BlobServiceClient object which will be used to create a container client """
        try:
            azure_blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        except Exception as ex:
            self.logger.error("Azure Blob Storage - Exception:" + str(ex))
        return azure_blob_service_client


    def image_upload(self, container, image_name, image_file=None, upload_file_path=None, des_folder="", is_remove=True):
        """ Upload image to azure and retrieve URL
        - container: Azure Storage Container destination (required)
        - image_name: name of the image (required)
        - des_folder: Folder within the Azure Storage Container (optional)
        - image_file: Actual image to save to file server (Unless it is already in the file server) (optional)
        - upload_file_path: temporary file server path where image will be stored before upload to azure.
                            Include image name (optional)
        """

        # str(uuid.uuid1())
        if ('.jpg' not in image_name) and ('.jpeg' not in image_name):
            image_name = image_name + '.jpeg'

        # Create a blob client using the local file name as the name for the blob
        if not des_folder == '':
            des_folder = des_folder + "/"
        azure_blob_client = self.service_client.get_blob_client(container=container,
                                                                blob=des_folder + image_name)
        # Save image locally in order to upload
        if upload_file_path is None:
            upload_file_path = os.path.join(pathlib.Path().resolve(), image_name)
        if image_file is not None:
            self.logger.info("Azure Blob Storage - Temporary local storage of " + image_name)
            # Set path to file
            image_file.save(upload_file_path, quality=95)

        try:
            self.logger.info("Azure Blob Storage - Uploading blob: " + image_name)
            image_content_setting = ContentSettings(content_type='image/jpeg')
            with open(upload_file_path, "rb") as data:
                azure_blob_client.upload_blob(data, overwrite=True, content_settings=image_content_setting)
            if is_remove:
                os.remove(upload_file_path)
            self.logger.info("Azure Blob Storage - Successful upload.")
            return azure_blob_client.url
        except Exception as ex:
            self.logger.error("Azure Blob Storage - Error uploading. Exception:" + str(ex))
            return 'Not applicable'

    def file_upload(self, container, file_name, file_path=pathlib.Path().resolve(), file=None, des_folder=""):
        """ Upload file to blog storage"""

        local_file_path = os.path.join(file_path, file_name)
        if file is not None:
            if ('.pkl' not in file_name):
                file_name = file_name + '.pkl'
                local_file_path = os.path.join(file_path, file_name)
            # Save to file system
            self.logger.info("Azure Blob Storage - Temporary local storage of " + file_name)
            file.to_pickle(local_file_path)

        # Create a blob client using the local file name as the name for the blob
        if not des_folder == '':
            des_folder = des_folder + "/"
        azure_blob_client = self.service_client.get_blob_client(container=container, blob=des_folder + file_name)

        try:
            self.logger.info("Azure Blob Storage - Uploading blob: " + file_name)
            with open(local_file_path, "rb") as data:
                azure_blob_client.upload_blob(data, overwrite=True)
            os.remove(local_file_path)
            self.logger.info("Azure Blob Storage - Successful upload.")
            return azure_blob_client.url
        except Exception as ex:
            self.logger.error("Azure Blob Storage - Error uploading. Exception:" + str(ex))
            return 'Not applicable'
            
    def delete_file(self, container, blob_name):
        self.logger.info("Azure Blob Storage - Removing blob: " + str(blob_name))
        # Instantiate a new ContainerClient
        container_client = self.service_client.get_container_client(container)
        try:
            # Instantiate a new BlobClient
            blob_client = container_client.get_blob_client(blob_name)
            # Delete Page Blob
            blob_client.delete_blob()
            self.logger.info("Azure Blob Storage - Successful delete.")
        except Exception as ex:
            self.logger.error("Azure Blob Storage - Could not delete blob. Exception:" + str(ex))

    def list_files(self, container, folder=None):
        blobfile = []
        # Create a container client
        container_client = self.service_client.get_container_client(container=container)
        if folder is not None:
            blobs_list = container_client.list_blobs(name_starts_with=folder)
        else:
            blobs_list = container_client.list_blobs()
        for blob in blobs_list:
            blobname = blob.name.split('/')[-1]
            blobfile.append(blobname)
        return blobfile

    def is_file_exist(self, container, folder, file):
        try:
            self.service_client.get_blob_properties(container, folder + '/' + file)
            self.logger.info('Azure Blob Storage - File already exist.')
            return True
        except Exception as e:
            self.logger.info('Azure Blob Storage - File DOES NOT exist.')
            return False

    def upload_to_file_share(self, file_path_origin, share_name, file_path_destination):
        file_client = ShareFileClient.from_connection_string(conn_str="<connection_string>", share_name=share_name, file_path=file_path_destination)
        with open(file_path_origin, "rb") as source_file:
            file_client.upload_file(source_file)


