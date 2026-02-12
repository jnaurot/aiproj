<script>
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import "@xyflow/svelte/dist/style.css";

	let { children } = $props();
</script>


{@render children()}
