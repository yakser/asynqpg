import { useState } from "react"

interface JsonTreeProps {
  data: unknown
  defaultOpen?: number
}

export function JsonTree({ data, defaultOpen = 2 }: JsonTreeProps) {
  return (
    <div className="json-tree">
      <JsonNode
        value={data}
        keyName={null}
        depth={0}
        defaultOpen={defaultOpen}
        isLast={true}
      />
    </div>
  )
}

interface JsonNodeProps {
  value: unknown
  keyName: string | number | null
  depth: number
  defaultOpen: number
  isLast: boolean
}

function JsonNode({
  value,
  keyName,
  depth,
  defaultOpen,
  isLast,
}: JsonNodeProps) {
  const isObj = value !== null && typeof value === "object"
  const isArr = Array.isArray(value)
  const [open, setOpen] = useState(depth < defaultOpen)

  if (!isObj) {
    return (
      <PrimitiveRow
        keyName={keyName}
        value={value}
        depth={depth}
        isLast={isLast}
      />
    )
  }

  const entries: Array<[string | number, unknown]> = isArr
    ? (value as unknown[]).map((v, i) => [i, v])
    : Object.entries(value as Record<string, unknown>)
  const openBrace = isArr ? "[" : "{"
  const closeBrace = isArr ? "]" : "}"
  const indent = depth * 14
  return (
    <div>
      <div className="jt-row" style={{ paddingLeft: indent }}>
        <span className="jt-toggle" onClick={() => setOpen((o) => !o)}>
          {open ? "▾" : "▸"}
        </span>
        {keyName != null && <span className="jt-key">"{keyName}"</span>}
        {keyName != null && <span className="jt-punct">:</span>}
        <span className="jt-punct">{openBrace}</span>
        {!open && (
          <span className="jt-comment">
            {entries.length} {isArr ? "item" : "key"}
            {entries.length === 1 ? "" : "s"}
          </span>
        )}
        {!open && <span className="jt-punct">{closeBrace}</span>}
        {!open && !isLast && <span className="jt-punct">,</span>}
      </div>
      {open &&
        entries.map(([k, v], i) => (
          <JsonNode
            key={String(k)}
            keyName={isArr ? null : k}
            value={v}
            depth={depth + 1}
            defaultOpen={defaultOpen}
            isLast={i === entries.length - 1}
          />
        ))}
      {open && (
        <div className="jt-row" style={{ paddingLeft: indent }}>
          <span className="jt-toggle">&nbsp;</span>
          <span className="jt-punct">
            {closeBrace}
            {!isLast ? "," : ""}
          </span>
        </div>
      )}
    </div>
  )
}

interface PrimitiveRowProps {
  keyName: string | number | null
  value: unknown
  depth: number
  isLast: boolean
}

function PrimitiveRow({ keyName, value, depth, isLast }: PrimitiveRowProps) {
  const indent = depth * 14
  let cls = "jt-null"
  let display = "null"
  if (typeof value === "string") {
    cls = "jt-str"
    display = `"${value}"`
  } else if (typeof value === "number") {
    cls = "jt-num"
    display = String(value)
  } else if (typeof value === "boolean") {
    cls = "jt-bool"
    display = String(value)
  } else if (value === null) {
    cls = "jt-null"
    display = "null"
  }
  return (
    <div className="jt-row" style={{ paddingLeft: indent }}>
      <span className="jt-toggle">&nbsp;</span>
      {keyName != null && <span className="jt-key">"{keyName}"</span>}
      {keyName != null && <span className="jt-punct">:&nbsp;</span>}
      <span className={cls}>{display}</span>
      {!isLast && <span className="jt-punct">,</span>}
    </div>
  )
}
