defmodule Commanded.SimpleProcessManagers.ProcessManager do
  use TelemetryRegistry

  telemetry_event(%{
    event: [:commanded, :process_manager, :handle, :start],
    description: "Emitted when a process manager starts handling an event",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Commanded.Application.t(),
      process_manager_name: String.t() | Inspect.t(),
      process_manager_module: module(),
      process_state: term(),
      process_uuid: String.t()}
    """
  })

  telemetry_event(%{
    event: [:commanded, :process_manager, :handle, :stop],
    description: "Emitted when a process manager stops handling an event",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Commanded.Application.t(),
      commands: [struct()],
      error: nil | any(),
      process_manager_name: String.t() | Inspect.t(),
      process_manager_module: module(),
      process_state: term(),
      process_uuid: String.t()}
    """
  })

  telemetry_event(%{
    event: [:commanded, :process_manager, :handle, :exception],
    description: "Emitted when a process manager raises an exception",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Commanded.Application.t(),
      process_manager_name: String.t() | Inspect.t(),
      process_manager_module: module(),
      process_state: term(),
      process_uuid: String.t(),
      kind: :throw | :error | :exit,
      reason: any(),
      stacktrace: list()}
    """
  })

  @type domain_event :: struct
  @type command :: struct
  @type process_manager :: struct
  @type process_uuid :: String.t()
  @type consistency :: :eventual | :strong
  @type handle_opts :: %{dispatch: fun(), metadata: map()}

  @callback init(config :: Keyword.t()) :: {:ok, Keyword.t()}

  @callback interested?(domain_event) ::
              {:start, process_uuid}
              | {:continue, process_uuid}
              | {:stop, process_uuid}
              | false

  @callback handle(process_manager, domain_event, handle_opts) ::
              {:ok, process_manager()} | {:error, any(), process_manager()}

  @optional_callbacks init: 1, interested?: 1, handle: 3

  alias Commanded.SimpleProcessManagers.ProcessManager
  alias Commanded.SimpleProcessManagers.ProcessRouter

  @doc false
  defmacro __using__(opts) do
    quote location: :keep do
      @before_compile unquote(__MODULE__)
      @behaviour ProcessManager
      @opts unquote(opts)

      def start_link(opts \\ []) do
        opts = Keyword.merge(@opts, opts)

        {application, name, config} = ProcessManager.parse_config!(__MODULE__, opts)

        ProcessRouter.start_link(application, name, __MODULE__, config)
      end

      def child_spec(opts) do
        opts = Keyword.merge(@opts, opts)

        {application, name, config} = ProcessManager.parse_config!(__MODULE__, opts)

        default = %{
          id: {__MODULE__, application, name},
          start: {ProcessRouter, :start_link, [application, name, __MODULE__, config]},
          restart: :permanent,
          type: :worker
        }

        Supervisor.child_spec(default, [])
      end

      @doc false
      def init(config), do: {:ok, config}

      defoverridable init: 1
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote generated: true do
      @doc false
      def interested?(_event), do: false

      @doc false
      def handle(process_manager, _event, _opts), do: {:ok, process_manager}
    end
  end

  defdelegate identity(), to: Commanded.SimpleProcessManagers.ProcessManagerInstance

  # GenServer start options
  @start_opts [:debug, :name, :timeout, :spawn_opt, :hibernate_after]

  # Process manager configuration options
  @handler_opts [
    :application,
    :name,
    :consistency,
    :start_from,
    :subscribe_to,
    :subscription_opts,
    :event_timeout,
    :idle_timeout
  ]

  def parse_config!(module, config) do
    {:ok, config} = module.init(config)

    {_valid, invalid} = Keyword.split(config, @start_opts ++ @handler_opts)

    if Enum.any?(invalid) do
      raise ArgumentError,
            inspect(module) <> " specifies invalid options: " <> inspect(Keyword.keys(invalid))
    end

    {application, config} = Keyword.pop(config, :application)

    unless application do
      raise ArgumentError, inspect(module) <> " expects :application option"
    end

    {name, config} = Keyword.pop(config, :name)
    name = parse_name(name)

    unless name do
      raise ArgumentError, inspect(module) <> " expects :name option"
    end

    {application, name, config}
  end

  @doc false
  def parse_name(name) when name in [nil, ""], do: nil
  def parse_name(name) when is_binary(name), do: name
  def parse_name(name), do: inspect(name)
end
