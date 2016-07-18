require "refile/spec_helper"
require "refile/azure"

WebMock.allow_net_connect!

config = YAML.load_file("azure.yml").map { |k, v| [k.to_sym, v] }.to_h

RSpec.describe Refile::Azure do
  let(:backend) { Refile::Azure.new(max_size: 100, **config) }

  it_behaves_like :backend
end
