<script lang="ts">
	// lib/flow/components/ui/Section.svelte
	import type { HTMLAttributes } from 'svelte/elements';
	
	interface SectionProps extends HTMLAttributes<HTMLDivElement> {
		title?: string;
	}
	
	export let title: string = '';
	export let className: string = '';
</script>

<div class="section {className}">
	{#if title}
		<div class="sectionTitle">{title}</div>
	{/if}
	
	<slot />
</div>

<style>
	.section {
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
	}
	
	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}
	
	@media (prefers-color-scheme: dark) {
		.section {
			border-color: rgba(255, 255, 255, 0.1);
			background: rgba(0, 0, 0, 0.3);
		}
	}
	
	@media (prefers-color-scheme: light) {
		.section {
			border-color: rgba(0, 0, 0, 0.1);
			background: rgba(255, 255, 255, 0.8);
		}
	}
</style>
