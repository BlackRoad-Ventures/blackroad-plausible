defmodule Plausible.Ingestion.Persistor.TCPClientPool do
  @moduledoc """
  Pool manager for TCP client
  """

  alias Plausible.Ingestion.Persistor.TCPClient

  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :name),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop!(opts, :name)
    {pool_size, opts} = Keyword.pop!(opts, :pool_size)
    opts = Keyword.update!(opts, :host, &String.to_charlist/1)
    registry_name = registry_name(name)

    client_specs =
      for index <- 1..pool_size do
        child_opts = Keyword.put(opts, :registry_name, registry_name)

        %{
          id: {:persistor_tcp_client, index},
          start: {TCPClient, :start_link, [child_opts]}
        }
      end

    children = [
      {Registry, keys: :duplicate, name: registry_name},
      %{
        id: :persistor_tcp_clients_supervisor,
        start: {
          Supervisor,
          :start_link,
          [client_specs, [strategy: :one_for_one]]
        }
      }
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: name)
  end

  @spec send(atom(), map(), map(), pos_integer() | nil) :: {:ok, map()} | {:error, term()}
  def send(pool_name, event, session_attrs, previous_user_id) do
    case Registry.lookup(registry_name(pool_name), :client) do
      [] ->
        {:error, :no_connections_available}

      pids ->
        {pid, _value} = Enum.random(pids)
        TCPClient.send(pid, event, session_attrs, previous_user_id)
    end
  end

  defp registry_name(pool_name) do
    Module.concat(pool_name, Registry)
  end
end
