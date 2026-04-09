<script lang="ts">
	export let text: string = '';
	export let tone: 'info' | 'warn' = 'info';
</script>

<div class={`hint ${tone === 'warn' ? 'warn' : 'info'}`}>{text}</div>

<style>
	.hint {
		font-size: 12px;
		margin-top: 6px;
	}

	.info {
		opacity: 0.78;
	}

	.warn {
		color: #fca5a5;
	}
</style>

