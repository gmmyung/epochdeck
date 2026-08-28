<script lang="ts">
  export let source: string;

  type Block =
    | { kind: "heading"; level: number; text: string }
    | { kind: "paragraph"; text: string }
    | { kind: "list"; items: string[] }
    | { kind: "code"; text: string };

  $: blocks = parseMarkdown(source);

  function parseMarkdown(markdown: string): Block[] {
    const result: Block[] = [];
    const lines = markdown.replaceAll("\r\n", "\n").split("\n");
    let index = 0;
    while (index < lines.length) {
      const line = lines[index];
      if (!line.trim()) {
        index += 1;
        continue;
      }
      if (line.startsWith("```")) {
        const code: string[] = [];
        index += 1;
        while (index < lines.length && !lines[index].startsWith("```")) {
          code.push(lines[index]);
          index += 1;
        }
        result.push({ kind: "code", text: code.join("\n") });
        index += 1;
        continue;
      }
      const heading = /^(#{1,4})\s+(.+)$/.exec(line);
      if (heading) {
        result.push({ kind: "heading", level: heading[1].length, text: heading[2] });
        index += 1;
        continue;
      }
      if (/^[-*]\s+/.test(line)) {
        const items: string[] = [];
        while (index < lines.length && /^[-*]\s+/.test(lines[index])) {
          items.push(lines[index].replace(/^[-*]\s+/, ""));
          index += 1;
        }
        result.push({ kind: "list", items });
        continue;
      }
      const paragraph = [line.trim()];
      index += 1;
      while (
        index < lines.length &&
        lines[index].trim() &&
        !/^(#{1,4})\s+/.test(lines[index]) &&
        !/^[-*]\s+/.test(lines[index]) &&
        !lines[index].startsWith("```")
      ) {
        paragraph.push(lines[index].trim());
        index += 1;
      }
      result.push({ kind: "paragraph", text: paragraph.join(" ") });
    }
    return result;
  }
</script>

<div class="markdown-body">
  {#each blocks as block}
    {#if block.kind === "heading"}
      {#if block.level === 1}<h2>{block.text}</h2>
      {:else if block.level === 2}<h3>{block.text}</h3>
      {:else}<h4>{block.text}</h4>{/if}
    {:else if block.kind === "paragraph"}
      <p>{block.text}</p>
    {:else if block.kind === "list"}
      <ul>
        {#each block.items as item}<li>{item}</li>{/each}
      </ul>
    {:else}
      <pre><code>{block.text}</code></pre>
    {/if}
  {/each}
</div>

<style>
  .markdown-body {
    color: var(--text);
    font-size: 12px;
    line-height: 1.6;
  }

  h2,
  h3,
  h4 {
    margin: 18px 0 7px;
    font-weight: 650;
    line-height: 1.3;
  }

  h2:first-child,
  h3:first-child,
  h4:first-child {
    margin-top: 0;
  }

  h2 {
    font-size: 18px;
  }

  h3 {
    font-size: 15px;
  }

  h4 {
    font-size: 13px;
  }

  p,
  ul {
    margin: 0 0 10px;
  }

  ul {
    padding-left: 20px;
  }

  pre {
    max-width: 100%;
    margin: 0 0 10px;
    padding: 10px;
    overflow: auto;
    border: 1px solid var(--line);
    background: var(--surface);
    font-size: 10px;
    line-height: 1.5;
  }
</style>
