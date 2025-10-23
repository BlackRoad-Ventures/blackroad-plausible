defmodule Plausible.Ingestion.Persistor.TCPClient do
  @moduledoc """
  Multiplexing TCP client for remote persistor
  """

  alias Plausible.Ingestion.Persistor.Telemetry

  require Logger

  @backoff_time 1_000

  # PRST
  @prefix <<80, 82, 83, 84>>

  @spec child_spec(keyword) :: Supervisor.child_spec()
  def child_spec(opts) do
    host =
      opts
      |> Keyword.get(:host)
      |> persistor_host()
      |> String.to_charlist()

    port =
      opts
      |> Keyword.get(:port)
      |> persistor_port()

    opts =
      opts
      |> Keyword.put(:host, host)
      |> Keyword.put(:port, port)

    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec start_link(keyword) :: :gen_statem.start_ret()
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, _gen_statem_opts = [])
  end

  @spec send(pid() | atom(), map(), map(), pos_integer() | nil, timeout()) ::
          {:ok, map()} | {:error, term()}
  def send(server_ref, event, session_attrs, previous_user_id, timeout \\ 15_000) do
    id = next_id()

    data =
      {event, session_attrs}
      |> :erlang.term_to_binary()

    payload =
      <<@prefix, id::integer-size(64), previous_user_id || 0::integer-size(64),
        byte_size(data)::integer-size(32), data::binary>>

    case :gen_statem.call(server_ref, {:send, id, payload}, timeout) do
      {:ok, response} ->
        if response.id != id do
          Logger.warning("Different response ID. Request id: #{id}. Response id: #{response.id}")
        end

        data = response.data |> :erlang.binary_to_term()
        {:ok, %{response | data: data}}

      other ->
        other
    end
  end

  @behaviour :gen_statem

  defstruct [:host, :port, :socket, :continuation, pending: %{}, timings: %{}]

  @impl :gen_statem
  def callback_mode, do: [:state_functions, :state_enter]

  @impl :gen_statem
  def init(opts) do
    if registry = opts[:registry_name] do
      {:ok, _} = Registry.register(registry, :client, :no_value)
    end

    data = %__MODULE__{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port)
    }

    ref = :atomics.new(1, [])
    :persistent_term.put({__MODULE__, :msg_counter}, ref)

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, data, actions}
  end

  ## Disconnected state

  def disconnected(:internal, :connect, data) do
    opts = [:binary, active: :once]

    case :gen_tcp.connect(data.host, data.port, opts, 5000) do
      {:ok, socket} ->
        data = %__MODULE__{data | socket: socket}
        {:next_state, :connected, data}

      {:error, reason} ->
        Logger.error("Failed to connect: #{:inet.format_error(reason)}")
        timer_action = {{:timeout, :reconnect}, @backoff_time, nil}
        {:keep_state_and_data, [timer_action]}
    end
  end

  def disconnected(:enter, :disconnected, _data) do
    :keep_state_and_data
  end

  def disconnected(:enter, :connected, data) do
    actions =
      for caller <- Map.values(data.pending) do
        {:reply, caller, {:error, :disconnected}}
      end

    data = %__MODULE__{data | pending: %{}, socket: nil, continuation: nil}
    {:keep_state, data, actions}
  end

  def disconnected({:timeout, :reconnect}, nil, _data) do
    actions = [{:next_event, :internal, :connect}]
    {:keep_state_and_data, actions}
  end

  def disconnected({:call, from}, {:send, _id, _payload}, _data) do
    actions = [{:reply, from, {:error, :disconnected}}]
    {:keep_state_and_data, actions}
  end

  ## Connected state

  def connected(:enter, _old_state = :disconnected, _data) do
    actions = [{{:timeout, :reconnect}, :cancel}]
    {:keep_state_and_data, actions}
  end

  def connected({:call, from}, {:send, id, payload}, data) do
    start_time = Telemetry.start(:tcp_request)
    :ok = :gen_tcp.send(data.socket, payload)
    data = update_in(data.pending, &Map.put(&1, id, from))
    data = update_in(data.timings, &Map.put(&1, id, start_time))
    {:keep_state, data}
  end

  def connected(:info, {:tcp, socket, bytes}, %__MODULE__{socket: socket} = data) do
    :ok = :inet.setopts(data.socket, active: :once)
    {data, actions} = handle_new_bytes(data, bytes)
    {:keep_state, data, actions}
  end

  def connected(:info, {:tcp_error, socket, reason}, %__MODULE__{socket: socket} = data) do
    Logger.error("Connection error: #{:inet.format_error(reason)}")
    {:next_state, :disconnected, data}
  end

  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = data) do
    Logger.error("Connection closed")
    {:next_state, :disconnected, data}
  end

  ## Helpers

  defp next_id() do
    ref = :persistent_term.get({__MODULE__, :msg_counter})

    :atomics.add_get(ref, 1, 1)
  end

  defp handle_new_bytes(data, bytes) do
    handle_new_bytes(data, bytes, _actions_acc = [])
  end

  defp handle_new_bytes(%__MODULE__{} = data, bytes, actions) do
    continuation = data.continuation || (&decode/1)

    case continuation.(bytes) do
      {:ok, response, rest} ->
        {start_time, data} = get_and_update_in(data.timings, &Map.pop(&1, response.id))

        if start_time do
          Telemetry.stop(:tcp_request, start_time)
        end

        data = %__MODULE__{data | continuation: nil}
        {caller, data} = get_and_update_in(data.pending, &Map.pop(&1, response.id))

        actions =
          if caller do
            [{:reply, caller, {:ok, response}} | actions]
          else
            Logger.warning("Caller missing when trying to send reply for #{response.id}")
            []
          end

        handle_new_bytes(data, rest, actions)

      {:continuation, continuation} ->
        {%__MODULE__{data | continuation: continuation}, actions}
    end
  end

  def decode(data) when byte_size(data) > 0 do
    decode_prefix(data, @prefix)
  end

  def decode(<<>>) do
    {:continuation, &decode/1}
  end

  defp decode_prefix(<<b, rest::binary>>, <<b, remaining::binary>>) do
    decode_prefix(rest, remaining)
  end

  defp decode_prefix(rest, <<>>) do
    decode_id(rest, 8, <<>>)
  end

  defp decode_prefix(<<>>, remaining) do
    {:continuation, &decode_prefix(&1, remaining)}
  end

  defp decode_id(<<b, rest::binary>>, n, acc) when n > 1 do
    decode_id(rest, n - 1, <<acc::binary, b>>)
  end

  defp decode_id(<<b, rest::binary>>, 1, acc) do
    <<id::integer-size(64)>> = <<acc::binary, b>>

    decode_size(rest, 4, <<>>, %{id: id})
  end

  defp decode_id(<<>>, n, acc) do
    {:continuation, &decode_id(&1, n, acc)}
  end

  defp decode_size(<<b, rest::binary>>, n, byte_acc, final_acc) when n > 1 do
    decode_size(rest, n - 1, <<byte_acc::binary, b>>, final_acc)
  end

  defp decode_size(<<b, rest::binary>>, 1, byte_acc, final_acc) do
    <<size::integer-size(32)>> = <<byte_acc::binary, b>>

    decode_data(rest, size, <<>>, Map.put(final_acc, :size, size))
  end

  defp decode_size(<<>>, n, byte_acc, final_acc) do
    {:continuation, &decode_size(&1, n, byte_acc, final_acc)}
  end

  defp decode_data(<<b, rest::binary>>, n, byte_acc, final_acc) when n > 1 do
    decode_data(rest, n - 1, <<byte_acc::binary, b>>, final_acc)
  end

  defp decode_data(<<b, rest::binary>>, 1, byte_acc, final_acc) do
    {:ok, Map.put(final_acc, :data, <<byte_acc::binary, b>>), rest}
  end

  defp decode_data(<<>>, n, byte_acc, final_acc) do
    {:continuation, &decode_data(&1, n, byte_acc, final_acc)}
  end

  defp persistor_host(nil) do
    Keyword.fetch!(Application.fetch_env!(:plausible, __MODULE__), :host)
  end

  defp persistor_host(host) when is_binary(host), do: host

  defp persistor_port(nil) do
    Keyword.fetch!(Application.fetch_env!(:plausible, __MODULE__), :port)
  end

  defp persistor_port(port) when is_binary(port), do: port
end
