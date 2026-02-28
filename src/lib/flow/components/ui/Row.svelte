<script lang="ts">
	export let className: string = '';
</script>

<div class="row {className}">
	<slot />
</div>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 110px auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}
</style>
