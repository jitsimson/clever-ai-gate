<script>
  import { onMount } from 'svelte';
  import { 
    Users, Plus, RefreshCw, Shield, AlertTriangle, Trash2, 
    Pencil, Copy, Check, X, Activity 
  } from '@lucide/svelte';
  import { appState } from '$lib/state.svelte.js';
  import Button from '$lib/components/Button.svelte';
  import Input from '$lib/components/Input.svelte';
  import Card from '$lib/components/Card.svelte';
  import Modal from '$lib/components/Modal.svelte';

  // ─── Local State ──────────────────────────────────────────────────────────
  let tenants = $state([]);
  let loading = $state(false);
  let error = $state('');

  // Add modal state
  let showAddModal = $state(false);
  let addForm = $state({ name: '', token_balance: 1000000000, rate_limit_rpm: 60 });
  let addLoading = $state(false);

  // Success API Key Modal
  let showKeyModal = $state(false);
  let createdTenant = $state({ name: '', api_key: '' });
  let keyCopied = $state(false);

  // Edit modal state
  let showEditModal = $state(false);
  let editForm = $state({ id: '', name: '', token_balance: 1000000000, rate_limit_rpm: 60, is_active: true });
  let editLoading = $state(false);

  // Delete modal state
  let showDeleteConfirm = $state(false);
  let deleteTargetId = $state(null);
  let deleteLoading = $state(false);

  // Auto-fetch when adminKey changes
  $effect(() => {
    const key = appState.getAdminKey();
    if (key && tenants.length === 0 && !loading) {
      loadTenants();
    }
  });

  onMount(() => {
    if (appState.getAdminKey()) {
      loadTenants();
    }
  });

  // ─── API Helper Headers ───────────────────────────────────────────────────
  function adminHeaders() {
    return {
      'Authorization': `Bearer ${appState.getAdminKey()}`,
      'Content-Type': 'application/json'
    };
  }

  async function loadTenants() {
    loading = true;
    error = '';
    appState.apiLoading = true;
    try {
      const res = await fetch('/api/v1/admin/tenants', { headers: adminHeaders() });
      if (res.ok) {
        tenants = await res.json();
      } else {
        const err = await res.json();
        error = err.error || `Error ${res.status}`;
      }
    } catch (e) {
      error = `Network error: ${e.message}`;
    } finally {
      loading = false;
      appState.apiLoading = false;
    }
  }

  function openAddModal() {
    addForm = { name: '', token_balance: 1000000000, rate_limit_rpm: 60 };
    showAddModal = true;
  }

  async function createTenant() {
    if (!addForm.name.trim()) {
      appState.addToast('error', 'Name is required');
      return;
    }
    addLoading = true;
    appState.apiLoading = true;
    try {
      const res = await fetch('/api/v1/admin/tenants', {
        method: 'POST',
        headers: adminHeaders(),
        body: JSON.stringify({
          name: addForm.name,
          token_balance: addForm.token_balance,
          rate_limit_rpm: addForm.rate_limit_rpm
        })
      });
      if (res.status === 201 || res.ok) {
        const data = await res.json();
        createdTenant = { name: data.name, api_key: data.api_key };
        appState.addToast('success', 'Tenant created successfully');
        showAddModal = false;
        showKeyModal = true;
        loadTenants();
      } else {
        const err = await res.json();
        appState.addToast('error', err.details || err.error || 'Failed to create tenant');
      }
    } catch (e) {
      appState.addToast('error', `Network error: ${e.message}`);
    } finally {
      addLoading = false;
      appState.apiLoading = false;
    }
  }

  function copyTenantKey() {
    navigator.clipboard.writeText(createdTenant.api_key);
    keyCopied = true;
    appState.addToast('success', 'Copied API key to clipboard');
    setTimeout(() => { keyCopied = false; }, 2000);
  }

  function openEditModal(tenant) {
    editForm = {
      id: tenant.id,
      name: tenant.name,
      token_balance: tenant.token_balance,
      rate_limit_rpm: tenant.rate_limit_rpm,
      is_active: tenant.is_active
    };
    showEditModal = true;
  }

  async function updateTenant() {
    if (!editForm.name.trim()) {
      appState.addToast('error', 'Name is required');
      return;
    }
    editLoading = true;
    appState.apiLoading = true;
    try {
      const res = await fetch(`/api/v1/admin/tenants/${editForm.id}`, {
        method: 'PUT',
        headers: adminHeaders(),
        body: JSON.stringify({
          name: editForm.name,
          token_balance: editForm.token_balance,
          rate_limit_rpm: editForm.rate_limit_rpm,
          is_active: editForm.is_active
        })
      });
      if (res.ok) {
        appState.addToast('success', 'Tenant updated successfully');
        showEditModal = false;
        loadTenants();
      } else {
        const err = await res.json();
        appState.addToast('error', err.details || err.error || 'Failed to update tenant');
      }
    } catch (e) {
      appState.addToast('error', `Network error: ${e.message}`);
    } finally {
      editLoading = false;
      appState.apiLoading = false;
    }
  }

  function confirmDelete(id) {
    deleteTargetId = id;
    showDeleteConfirm = true;
  }

  async function deleteTenantById() {
    deleteLoading = true;
    appState.apiLoading = true;
    try {
      const res = await fetch(`/api/v1/admin/tenants/${deleteTargetId}`, {
        method: 'DELETE',
        headers: adminHeaders()
      });
      if (res.ok) {
        appState.addToast('success', 'Tenant deleted successfully');
        showDeleteConfirm = false;
        deleteTargetId = null;
        loadTenants();
      } else {
        const err = await res.json();
        appState.addToast('error', err.details || err.error || 'Failed to delete tenant');
      }
    } catch (e) {
      appState.addToast('error', `Network error: ${e.message}`);
    } finally {
      deleteLoading = false;
      appState.apiLoading = false;
    }
  }

  function connectAdminKey() {
    const key = appState.adminKey.trim();
    if (!key) return;
    localStorage.setItem('cag_admin_key', key);
    loadTenants();
  }

  onMount(() => {
    if (appState.adminKey.trim()) {
      loadTenants();
    }
  });
</script>

<header class="header flex items-center justify-between px-6 py-4 border-b shrink-0">
  <div class="flex items-center gap-3">
    <Users size={20} class="text-[#f97316]" />
    <span class="font-bold text-base">Tenant Accounts</span>
    {#if appState.adminKey.trim()}
      <span class="text-xs font-bold text-secondary bg-gray-500/10 border border-gray-500/20 px-2.5 py-0.5 rounded-full uppercase">{tenants.length} tenants</span>
    {/if}
  </div>
  
  {#if appState.adminKey.trim()}
    <div class="flex items-center gap-2 animate-fade-in">
      <Button variant="secondary" size="sm" onclick={() => { loadTenants(); appState.addToast('info', 'Refreshing tenants...'); }}>
        <RefreshCw size={14} />
        Refresh
      </Button>
      <Button variant="primary" size="sm" onclick={openAddModal}>
        <Plus size={14} />
        Create Tenant
      </Button>
    </div>
  {/if}
</header>

{#if !appState.adminKey.trim()}
  <!-- Admin key prompt -->
  <div class="logs-key-prompt flex flex-col justify-center items-center flex-grow p-6">
    <Card variant="filled" padding="lg" class="logs-key-card flex flex-col items-center text-center">
      <Shield size={40} class="text-[#f97316] mb-4 animate-pulse" />
      <h2 class="font-bold text-lg mb-2 text-primary">Admin Key Required</h2>
      <p class="text-sm mb-6 text-secondary max-w-sm">Enter your Admin API Key to manage tenant accounts, virtual routing keys, and usage statistics.</p>
      
      <div class="flex flex-col gap-3 w-full max-w-sm">
        <Input
          type="password"
          placeholder="Enter Admin API Key..."
          bind:value={appState.adminKey}
          onkeydown={(e) => { if (e.key === 'Enter') connectAdminKey(); }}
        />
        <Button variant="primary" size="md" onclick={connectAdminKey}>
          Connect
        </Button>
      </div>
      
      {#if error}
        <p class="text-red-500 text-sm font-semibold mt-4">{error}</p>
      {/if}
    </Card>
  </div>
{:else}
  <!-- Tenants data grid -->
  <div class="providers-grid-wrap flex-grow overflow-auto p-6">
    {#if loading}
      <div class="providers-loading flex flex-col items-center justify-center h-64">
        <div class="animate-spin text-[#f97316] text-xl">⟳</div>
        <p class="text-sm mt-2 text-secondary">Loading tenant accounts...</p>
      </div>
    {:else if error}
      <div class="providers-loading flex flex-col items-center justify-center h-64">
        <AlertTriangle size={40} class="text-red-500 mb-2" />
        <p class="text-red-500 text-sm font-semibold">{error}</p>
        <Button variant="primary" class="mt-4" onclick={loadTenants}>Retry</Button>
      </div>
    {:else if tenants.length === 0}
      <div class="providers-loading flex flex-col items-center justify-center h-64">
        <Users size={48} class="opacity-20 mb-4" />
        <p class="opacity-50 text-sm text-secondary">No tenants registered yet.</p>
        <Button variant="primary" class="mt-4" onclick={openAddModal}>
          <Plus size={14} /> Create First Tenant
        </Button>
      </div>
    {:else}
      <div class="providers-table-container">
        <table class="providers-table">
          <thead>
            <tr>
              <th style="font-size: 11px;">ID</th>
              <th style="font-size: 11px;">Name</th>
              <th style="font-size: 11px;">Token Balance</th>
              <th style="font-size: 11px;">Rate Limit (RPM)</th>
              <th style="font-size: 11px;">Status</th>
              <th style="font-size: 11px;">API Key</th>
              <th style="font-size: 11px; text-align: center;">Actions</th>
            </tr>
          </thead>
          <tbody>
            {#each tenants as tenant (tenant.id)}
              <tr class="provider-row">
                <td class="font-mono text-xs opacity-60" title={tenant.id}>#{tenant.id.slice(0, 8)}...</td>
                <td class="font-bold text-sm">{tenant.name}</td>
                <td class="font-mono text-sm">{appState.formatBalance(tenant.token_balance)}</td>
                <td class="font-mono text-sm">{tenant.rate_limit_rpm || 'No Limit'}</td>
                <td>
                  <span class="provider-badge {tenant.is_active ? 'badge-openai' : 'badge-default'}">
                    {tenant.is_active ? 'Active' : 'Suspended'}
                  </span>
                </td>
                <td class="font-mono text-xs opacity-60">{tenant.api_key}</td>
                <td>
                  <div class="flex items-center justify-center gap-1">
                    <Button variant="ghost" size="sm" onclick={() => openEditModal(tenant)} title="Edit tenant">
                      <Pencil size={15} />
                    </Button>
                    <Button variant="ghost" size="sm" onclick={() => confirmDelete(tenant.id)} title="Delete tenant">
                      <Trash2 size={15} class="text-red-500" />
                    </Button>
                  </div>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
{/if}

<!-- ─── CREATE TENANT MODAL ────────────────────────────────────────────────── -->
<Modal bind:show={showAddModal} title="Create Tenant">
  <div class="flex flex-col gap-4">
    <Input 
      type="text" 
      label="Tenant Name" 
      placeholder="Acme Corp, Mobile Client..." 
      bind:value={addForm.name} 
    />
    <div class="flex flex-col gap-1.5">
      <div class="flex justify-between items-center">
        <span class="text-xs font-bold uppercase tracking-wider text-secondary">Token Balance</span>
        <button 
          type="button" 
          class="text-xs text-primary hover:underline cursor-pointer"
          onclick={() => addForm.token_balance = 9000000000000000}>
          Set Unlimited
        </button>
      </div>
      <Input 
        type="number" 
        bind:value={addForm.token_balance} 
      />
    </div>
    <div class="flex flex-col gap-1.5">
      <div class="flex justify-between items-center">
        <span class="text-xs font-bold uppercase tracking-wider text-secondary">Rate Limit (RPM)</span>
        <button 
          type="button" 
          class="text-xs text-primary hover:underline cursor-pointer"
          onclick={() => addForm.rate_limit_rpm = 2000000000}>
          Set Unlimited
        </button>
      </div>
      <Input 
        type="number" 
        bind:value={addForm.rate_limit_rpm} 
      />
    </div>
  </div>

  {#snippet footer()}
    <div class="flex justify-end gap-3 w-full">
      <Button variant="outline" onclick={() => showAddModal = false}>Cancel</Button>
      <Button variant="primary" onclick={createTenant} disabled={addLoading}>
        {#if addLoading}
          <span class="animate-spin">⟳</span> Creating...
        {:else}
          Create Tenant
        {/if}
      </Button>
    </div>
  {/snippet}
</Modal>

<!-- ─── SUCCESS KEY MODAL ──────────────────────────────────────────────────── -->
<Modal bind:show={showKeyModal} title="Tenant Created!">
  <div class="flex flex-col items-center gap-4 text-center">
    <div class="w-12 h-12 rounded-full bg-green-500/10 border border-green-500/30 flex items-center justify-center text-green-500">
      <Check size={24} />
    </div>
    <p class="text-sm text-secondary">
      Tenant <strong>{createdTenant.name}</strong> was created successfully. Copy their API Key below. You will not be able to see it again.
    </p>

    <div class="flex items-center gap-2 w-full bg-surface-2 p-2.5 rounded-lg border border-border font-mono text-xs text-primary break-all select-all">
      <span class="flex-grow text-left">{createdTenant.api_key}</span>
      <Button variant="secondary" size="sm" onclick={copyTenantKey} class="shrink-0">
        {#if keyCopied}
          <Check size={16} class="text-green-500" />
        {:else}
          <Copy size={16} />
        {/if}
      </Button>
    </div>
  </div>

  {#snippet footer()}
    <div class="flex justify-center w-full">
      <Button variant="primary" onclick={() => showKeyModal = false}>
        Done
      </Button>
    </div>
  {/snippet}
</Modal>

<!-- ─── EDIT TENANT MODAL ──────────────────────────────────────────────────── -->
<Modal bind:show={showEditModal} title="Edit Tenant">
  <div class="flex flex-col gap-4">
    <Input 
      type="text" 
      label="Tenant Name" 
      bind:value={editForm.name} 
    />
    <div class="flex flex-col gap-1.5">
      <div class="flex justify-between items-center">
        <span class="text-xs font-bold uppercase tracking-wider text-secondary">Token Balance</span>
        <button 
          type="button" 
          class="text-xs text-primary hover:underline cursor-pointer"
          onclick={() => editForm.token_balance = 9000000000000000}>
          Set Unlimited
        </button>
      </div>
      <Input 
        type="number" 
        bind:value={editForm.token_balance} 
      />
    </div>
    
    <div class="flex gap-4 items-end">
      <div class="flex-grow flex flex-col gap-1.5">
        <div class="flex justify-between items-center">
          <span class="text-xs font-bold uppercase tracking-wider text-secondary">Rate Limit (RPM)</span>
          <button 
            type="button" 
            class="text-xs text-primary hover:underline cursor-pointer"
            onclick={() => editForm.rate_limit_rpm = 2000000000}>
            Set Unlimited
          </button>
        </div>
        <Input 
          type="number" 
          bind:value={editForm.rate_limit_rpm} 
        />
      </div>
      
      <div class="flex flex-col gap-2 shrink-0">
        <span class="text-xs font-bold uppercase tracking-wider text-secondary">Status</span>
        <label class="toggle-switch" style="margin-bottom: 9px;">
          <input type="checkbox" bind:checked={editForm.is_active} />
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>
  </div>

  {#snippet footer()}
    <div class="flex justify-end gap-3 w-full">
      <Button variant="outline" onclick={() => showEditModal = false}>Cancel</Button>
      <Button variant="primary" onclick={updateTenant} disabled={editLoading}>
        {#if editLoading}
          <span class="animate-spin">⟳</span> Saving...
        {:else}
          Save Changes
        {/if}
      </Button>
    </div>
  {/snippet}
</Modal>

<!-- ─── DELETE CONFIRMATION DIALOG ─────────────────────────────────────────── -->
<Modal bind:show={showDeleteConfirm} title="Delete Tenant?">
  <div class="flex flex-col items-center gap-4 text-center">
    <AlertTriangle size={48} class="text-red-500 mb-2" />
    <p class="text-sm text-secondary">
      All virtual routing credentials, statistics, and playground histories associated with this tenant account will be deleted permanently.
    </p>
    <p class="text-xs text-red-500 font-bold">This action is permanent and cannot be undone.</p>
  </div>

  {#snippet footer()}
    <div class="flex justify-center gap-3 w-full">
      <Button variant="outline" onclick={() => { showDeleteConfirm = false; deleteTargetId = null; }}>Cancel</Button>
      <Button variant="danger" onclick={deleteTenantById} disabled={deleteLoading}>
        {#if deleteLoading}
          <span class="animate-spin">⟳</span>
        {:else}
          Delete Permanently
        {/if}
      </Button>
    </div>
  {/snippet}
</Modal>

<style>

</style>
