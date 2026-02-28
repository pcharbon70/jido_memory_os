defmodule Jido.MemoryOS do
  @moduledoc """
  Public facade for MemoryOS operations.

  This module delegates to `Jido.MemoryOS.MemoryManager` so callers can keep
  using a stable API while internals evolve in later phases.
  """

  alias Jido.MemoryOS.{MemoryManager, Migration}

  @type target :: map() | struct()
  @type attrs :: map() | keyword()
  @type query :: map() | keyword() | Jido.Memory.Query.t() | Jido.MemoryOS.Query.t()

  @doc """
  Writes a memory record into the configured tier.
  """
  @spec remember(target(), attrs(), keyword()) :: {:ok, Jido.Memory.Record.t()} | {:error, term()}
  def remember(target, attrs, opts \\ []), do: MemoryManager.remember(target, attrs, opts)

  @doc """
  Retrieves memory records using structured filters.
  """
  @spec retrieve(target(), query(), keyword()) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  def retrieve(target, query, opts \\ []), do: MemoryManager.retrieve(target, query, opts)

  @doc """
  Deletes one memory record by id.
  """
  @spec forget(target(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def forget(target, id, opts \\ []), do: MemoryManager.forget(target, id, opts)

  @doc """
  Prunes expired records in configured stores.
  """
  @spec prune(target(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune(target, opts \\ []), do: MemoryManager.prune(target, opts)

  @doc """
  Placeholder consolidation entrypoint for later lifecycle phases.
  """
  @spec consolidate(target(), keyword()) :: {:ok, map()} | {:error, term()}
  def consolidate(target, opts \\ []), do: MemoryManager.consolidate(target, opts)

  @doc """
  Returns retrieval debug details and current query result metadata.
  """
  @spec explain_retrieval(target(), query(), keyword()) :: {:ok, map()} | {:error, term()}
  def explain_retrieval(target, query, opts \\ []),
    do: MemoryManager.explain_retrieval(target, query, opts)

  @doc """
  Migrates legacy `jido_memory` records for one target into MemoryOS tiers.
  """
  @spec migrate_legacy(target(), keyword()) :: {:ok, map()} | {:error, term()}
  def migrate_legacy(target, opts \\ []), do: Migration.migrate_target(target, opts)

  @doc """
  Compares legacy and MemoryOS record counts for one target.
  """
  @spec reconcile_legacy(target(), keyword()) :: {:ok, map()} | {:error, term()}
  def reconcile_legacy(target, opts \\ []), do: Migration.reconcile_target(target, opts)
end
