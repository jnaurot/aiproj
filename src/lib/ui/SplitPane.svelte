<script lang="ts">
  export let leftWidth = 70; // %
</script>

<div class="wrap">
  <div class="left" style={`width:${leftWidth}%`}>
    <slot name="left" />
  </div>
  <div class="right" style={`width:${100 - leftWidth}%`}>
    <slot name="right" />
  </div>
</div>

<style>
  .wrap {
    display: flex;
    height: 100vh;
    background: #0b0c10;
  }
  .left, .right {
    height: 100%;
    overflow: hidden;
  }
  .right {
    border-left: 1px solid #1f2430;
    overflow: auto;
  }
</style>
