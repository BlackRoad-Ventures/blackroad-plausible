defmodule Plausible.Ingestion.Persistor.RemoteOverTCP do
  @moduledoc """
  TCP version of remote client
  """

  alias Plausible.Ingestion.Persistor.TCPClientPool

  require Logger

  def persist_event(ingest_event, previous_user_id, _opts) do
    event = ingest_event.clickhouse_event
    session_attrs = ingest_event.clickhouse_session_attrs

    site_id = event.site_id
    current_user_id = event.user_id

    with {:ok, %{data: response}} <- request(event, session_attrs, previous_user_id) do
      case response do
        %{} = event_data ->
          case decode_payload(event_data) do
            {:ok, event} ->
              {:ok, %{ingest_event | clickhouse_event: event}}

            {:error, error} ->
              log_error(site_id, current_user_id, previous_user_id, {:error, error})

              {:error, :persist_error}
          end

        {:error, :no_session_for_engagement} ->
          {:error, :no_session_for_engagement}

        {:error, error} ->
          log_error(site_id, current_user_id, previous_user_id, {:error, error})

          {:error, :persist_error}
      end
    else
      error ->
        log_error(site_id, current_user_id, previous_user_id, error)
        {:error, :persist_error}
    end
  end

  def telemetry_request_duration() do
    [:plausible, :remote_ingest, :request, :duration]
  end

  defp request(event, session_attrs, previous_user_id) do
    Plausible.PromEx.Plugins.PlausibleMetrics.measure_duration(
      telemetry_request_duration(),
      fn ->
        TCPClientPool.send(TCPClientPool, event, session_attrs, previous_user_id)
      end
    )
  end

  defp decode_payload(event_data) do
    event_data = Map.drop(event_data, [:__struct__, :__meta__])
    event = struct(Plausible.ClickhouseEventV2, event_data)

    {:ok, event}
  catch
    _, _ ->
      {:error, :invalid_payload}
  end

  defp log_error(site_id, current_user_id, previous_user_id, error) do
    Logger.warning(
      "Persisting event for (#{site_id};#{current_user_id},#{previous_user_id}) failed: #{inspect(error)}"
    )
  end
end
