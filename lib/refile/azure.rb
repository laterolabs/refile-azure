require "azure"
require "open-uri"
require "refile"
require "refile/backend_macros"
require "refile/azure/version"

module Refile

  # @api private
  class AzureBackendError < StandardError; end

  # @api private
  class AzureCredentialsError < AzureBackendError
    def message
      "Credentials not found"
    end
  end

  # A refile backend which stores files in Microsoft Azure
  #
  # @example
  #   backend = Refile::Backend::Azure.new(
  #     storage_account_name: 'mystorage',
  #     storage_access_key: 'secret_key',
  #     container: "my-container",
  #   )
  #   file = backend.upload(StringIO.new("hello"))
  #   backend.read(file.id) # => "hello"
  class Azure
    extend Refile::BackendMacros

    attr_reader :storage_account_name, :container, :max_size

    # Sets up an Azure backend
    #
    # @param [String] container         The name of the container where files will be stored
    # @param [String] prefix            A prefix to add to all files.
    # @param [Integer, nil] max_size    The maximum size of an uploaded file
    # @param [#hash] hasher             A hasher which is used to generate ids from files
    # @param [Hash] azure_options       Additional options to initialize Azure Client with
    # @see https://github.com/Azure/azure-sdk-for-ruby
    def initialize(storage_account_name:, storage_access_key:, container:, max_size: nil, hasher: Refile::RandomHasher.new, **azure_options)
      @azure_options = azure_options
      @azure = ::Azure.client(
        azure_options.merge(
          storage_account_name: storage_account_name,
          storage_access_key: storage_access_key,
          storage_blob_host: "https://#{storage_account_name}.blob.core.windows.net"
        )
      )
      @storage_account_name = storage_account_name
      @container = container
      @blobs = @azure.blobs
      @hasher = hasher
      @max_size = max_size
    end

    # Upload a file into this backend
    #
    # @param [IO] uploadable      An uploadable IO-like object.
    # @return [Refile::File]      The uploaded file
    verify_uploadable def upload(uploadable)
      id = @hasher.hash(uploadable)

      if uploadable.is_a?(Refile::File) and uploadable.backend.is_a?(Azure) and uploadable.backend.storage_account_name == storage_account_name
        @blobs.copy_blob(@container, id, uploadable.backend.container, uploadable.id)
      else
        body = if IO === uploadable
          uploadable
        elsif uploadable.respond_to?(:read)
          uploadable.read
        else
          uploadable
        end
        if uploadable.try(:content_type)
          @blobs.create_block_blob(@container, id, body, {content_type: uploadable.content_type})
        else
          content_type = MIME::Types.type_for(uploadable.path)[0].content_type
          @blobs.create_block_blob(@container, id, body, {content_type: content_type})
        end
      end

      Refile::File.new(self, id)
    end

    # Get a file from this backend.
    #
    # Note that this method will always return a {Refile::File} object, even
    # if a file with the given id does not exist in this backend. Use
    # {FileSystem#exists?} to check if the file actually exists.
    #
    # @param [String] id           The id of the file
    # @return [Refile::File]      The retrieved file
    verify_id def get(id)
      Refile::File.new(self, id)
    end

    # Delete a file from this backend
    #
    # @param [String] id           The id of the file
    # @return [void]
    verify_id def delete(id)
      @blobs.delete_blob(@container, id)
    rescue ::Azure::Core::Http::HTTPError => exc
      raise exc unless exc.status_code == 404
      nil
    end

    # Return an IO object for the uploaded file which can be used to read its
    # content.
    #
    # @param [String] id           The id of the file
    # @return [IO]                An IO object containing the file contents
    verify_id def open(id)
      StringIO.new(read(id))
    end

    # Return the entire contents of the uploaded file as a String.
    #
    # @param [String] id           The id of the file
    # @return [String]             The file's contents
    verify_id def read(id)
      blob, body = @blobs.get_blob(@container, id)
      body
    rescue ::Azure::Core::Http::HTTPError => exc
      raise exc unless exc.status_code == 404
      nil
    end

    # Return the size in bytes of the uploaded file.
    #
    # @param [String] id           The id of the file
    # @return [Integer]           The file's size
    verify_id def size(id)
      @blobs.get_blob_properties(@container, id).properties[:content_length]
    rescue ::Azure::Core::Http::HTTPError => exc
      raise exc unless exc.status_code == 404
      nil
    end

    # Return whether the file with the given id exists in this backend.
    #
    # @param [String] id           The id of the file
    # @return [Boolean]
    verify_id def exists?(id)
      @blobs.get_blob_properties(@container, id)
      true
    rescue ::Azure::Core::Http::HTTPError => exc
      raise exc unless exc.status_code == 404
      false
    end

    # Remove all files in this backend. You must confirm the deletion by
    # passing the symbol `:confirm` as an argument to this method.
    #
    # @example
    #   backend.clear!(:confirm)
    # @raise [Refile::Confirm]     Unless the `:confirm` symbol has been passed.
    # @param [:confirm] confirm    Pass the symbol `:confirm` to confirm deletion.
    # @return [void]
    def clear!(confirm = nil)
      raise Refile::Confirm unless confirm == :confirm
      @blobs.list_blobs(@container).each do |blob|
        @blobs.delete_blob(@container, blob.name)
      end

    end

  end
end
