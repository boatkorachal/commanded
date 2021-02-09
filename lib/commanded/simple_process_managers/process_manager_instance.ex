defmodule Commanded.SimpleProcessManagers.ProcessManagerInstance do
  @moduledoc false

  use GenServer, restart: :temporary

  require Logger

  alias Commanded.Application
  alias Commanded.SimpleProcessManagers.{ProcessRouter}
  alias Commanded.EventStore
  alias Commanded.EventStore.{RecordedEvent, SnapshotData}
  alias Commanded.Telemetry

  defmodule State do
    @moduledoc false

    defstruct [
      :application,
      :idle_timeout,
      :process_router,
      :process_manager_name,
      :process_manager_module,
      :process_uuid,
      :process_state,
      :last_seen_event
    ]
  end

  def start_link(opts) do
    process_manager_module = Keyword.fetch!(opts, :process_manager_module)

    state = %State{
      application: Keyword.fetch!(opts, :application),
      idle_timeout: Keyword.fetch!(opts, :idle_timeout),
      process_router: Keyword.fetch!(opts, :process_router),
      process_manager_name: Keyword.fetch!(opts, :process_manager_name),
      process_manager_module: process_manager_module,
      process_uuid: Keyword.fetch!(opts, :process_uuid),
      process_state: struct(process_manager_module)
    }

    GenServer.start_link(__MODULE__, state)
  end

  @doc """
  Checks whether or not the process manager has already processed events
  """
  def new?(instance) do
    GenServer.call(instance, :new?)
  end

  @doc """
  Handle the given event by delegating to the process manager module
  """
  def process_event(instance, %RecordedEvent{} = event) do
    GenServer.cast(instance, {:process_event, event})
  end

  @doc """
  Stop the given process manager and delete its persisted state.

  Typically called when it has reached its final state.
  """
  def stop(instance) do
    GenServer.call(instance, :stop)
  end

  @doc """
  Fetch the process state of this instance
  """
  def process_state(instance) do
    GenServer.call(instance, :process_state)
  end

  @doc """
  Get the current process manager instance's identity.
  """
  def identity, do: Process.get(:process_uuid)

  @doc false
  @impl GenServer
  def init(%State{} = state) do
    {:ok, state, {:continue, :fetch_state}}
  end

  @doc """
  Attempt to fetch intial process state from snapshot storage.
  """
  @impl GenServer
  def handle_continue(:fetch_state, %State{} = state) do
    %State{application: application, process_uuid: process_uuid} = state

    state =
      case EventStore.read_snapshot(application, snapshot_uuid(state)) do
        {:ok, snapshot} ->
          %State{
            state
            | process_state: snapshot.data,
              last_seen_event: snapshot.source_version
          }

        {:error, :snapshot_not_found} ->
          state
      end

    Process.put(:process_uuid, process_uuid)

    {:noreply, state}
  end

  @doc false
  @impl GenServer
  def handle_call(:stop, _from, %State{} = state) do
    # Stop the process with a normal reason
    {:stop, :normal, :ok, state}
  end

  @doc false
  @impl GenServer
  def handle_call(:process_state, _from, %State{} = state) do
    %State{idle_timeout: idle_timeout, process_state: process_state} = state

    {:reply, process_state, state, idle_timeout}
  end

  @doc false
  @impl GenServer
  def handle_call(:new?, _from, %State{} = state) do
    %State{idle_timeout: idle_timeout, last_seen_event: last_seen_event} = state

    {:reply, is_nil(last_seen_event), state, idle_timeout}
  end

  @doc """
  Handle the given event, using the process manager module, against the current process state
  """
  @impl GenServer
  def handle_cast({:process_event, event}, %State{} = state) do
    if event_already_seen?(event, state) do
      process_seen_event(event, state)
    else
      process_unseen_event(event, state)
    end
  end

  @doc false
  @impl GenServer
  def handle_info(:timeout, %State{} = state) do
    Logger.debug(fn -> describe(state) <> " stopping due to inactivity timeout" end)

    {:stop, :normal, state}
  end

  @doc false
  @impl GenServer
  def handle_info(message, state) do
    Logger.error(fn -> describe(state) <> " received unexpected message: " <> inspect(message) end)

    {:noreply, state}
  end

  defp event_already_seen?(%RecordedEvent{}, %State{last_seen_event: nil}),
    do: false

  defp event_already_seen?(%RecordedEvent{} = event, %State{} = state) do
    %RecordedEvent{event_number: event_number} = event
    %State{last_seen_event: last_seen_event} = state

    event_number <= last_seen_event
  end

  # Already seen event, so just ack.
  defp process_seen_event(%RecordedEvent{} = event, %State{} = state) do
    %State{idle_timeout: idle_timeout} = state

    :ok = ack_event(event, state)

    {:noreply, state, idle_timeout}
  end

  defp process_unseen_event(%RecordedEvent{} = event, %State{} = state) do
    %RecordedEvent{
      event_number: event_number,
      data: data,
      event_id: event_id,
      correlation_id: correlation_id
    } = event

    %State{
      application: application,
      process_manager_module: process_manager_module,
      process_state: process_state,
      last_seen_event: current_event_number,
      idle_timeout: idle_timeout
    } = state

    telemetry_metadata = telemetry_metadata(event, state)
    start_time = telemetry_start(telemetry_metadata)

    dispatch = fn command ->
      opts = [causation_id: event_id, correlation_id: correlation_id, returning: false]
      Application.dispatch(application, command, opts)
    end

    metadata = RecordedEvent.enrich_metadata(event, [])

    opts = %{dispatch: dispatch, metadata: metadata}

    try do
      case process_manager_module.handle(process_state, data, opts) do
        {:ok, process_state} ->
          telemetry_stop(start_time, telemetry_metadata, {:ok, process_state})

          state = %State{
            state
            | process_state: process_state,
              last_seen_event: event_number
          }

          :ok = persist_state(event_number, state)
          :ok = ack_event(event, state)

          {:noreply, state, idle_timeout}

        {:error, error, process_state} ->
          telemetry_stop(start_time, telemetry_metadata, {:error, error})

          # save state without ack event
          state = %State{
            state
            | process_state: process_state
          }

          :ok = persist_state(current_event_number, state)

          {:stop, error, state}

        {:error, error} ->
          telemetry_stop(start_time, telemetry_metadata, {:error, error})

          {:stop, error, state}
      end
    rescue
      error ->
        telemetry_stop(start_time, telemetry_metadata, {:error, error})

        stacktrace = __STACKTRACE__
        Logger.error(fn -> Exception.format(:error, error, stacktrace) end)

        {:stop, error, state}
    end
  end

  defp describe(%State{process_manager_module: process_manager_module}),
    do: inspect(process_manager_module)

  defp persist_state(source_version, %State{} = state) do
    %State{
      application: application,
      process_manager_module: process_manager_module,
      process_state: process_state
    } = state

    snapshot = %SnapshotData{
      source_uuid: snapshot_uuid(state),
      source_version: source_version,
      source_type: Atom.to_string(process_manager_module),
      data: process_state
    }

    EventStore.record_snapshot(application, snapshot)
  end

  defp ack_event(%RecordedEvent{} = event, %State{} = state) do
    %State{process_router: process_router} = state

    ProcessRouter.ack_event(process_router, event, self())
  end

  defp snapshot_uuid(%State{} = state) do
    %State{process_manager_name: process_manager_name, process_uuid: process_uuid} = state

    inspect(process_manager_name) <> "-" <> inspect(process_uuid)
  end

  defp telemetry_start(telemetry_metadata) do
    Telemetry.start([:commanded, :process_manager, :handle], telemetry_metadata)
  end

  defp telemetry_stop(start_time, telemetry_metadata, handle_result) do
    event_prefix = [:commanded, :process_manager, :handle]

    case handle_result do
      {:ok, _state} ->
        telemetry_metadata = telemetry_metadata |> Map.put(:error, nil)

        Telemetry.stop(event_prefix, start_time, telemetry_metadata)

      {:error, error} ->
        telemetry_metadata =
          telemetry_metadata
          |> Map.put(:error, error)

        Telemetry.stop(event_prefix, start_time, telemetry_metadata)

      {:error, error, stacktrace} ->
        Telemetry.exception(
          event_prefix,
          start_time,
          :error,
          error,
          stacktrace,
          telemetry_metadata
        )
    end
  end

  defp telemetry_metadata(%RecordedEvent{} = event, %State{} = state) do
    %State{
      application: application,
      process_manager_name: process_manager_name,
      process_manager_module: process_manager_module,
      process_state: process_state,
      process_uuid: process_uuid
    } = state

    %{
      application: application,
      process_manager_name: process_manager_name,
      process_manager_module: process_manager_module,
      process_state: process_state,
      process_uuid: process_uuid,
      recorded_event: event
    }
  end
end
