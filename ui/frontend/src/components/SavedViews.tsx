import { fmtN } from "@/lib/format"

export interface SavedView<T extends string> {
  id: T
  label: string
  count?: number
}

interface SavedViewsProps<T extends string> {
  views: SavedView<T>[]
  active: T
  onChange: (id: T) => void
}

export function SavedViews<T extends string>({
  views,
  active,
  onChange,
}: SavedViewsProps<T>) {
  return (
    <div className="views-row" role="tablist" aria-label="Saved views">
      {views.map((v) => {
        const isActive = active === v.id
        return (
          <button
            key={v.id}
            type="button"
            role="tab"
            aria-selected={isActive}
            className={"v" + (isActive ? " active" : "")}
            onClick={() => onChange(v.id)}
          >
            {v.label}
            {v.count != null && <span className="ct">{fmtN(v.count)}</span>}
          </button>
        )
      })}
    </div>
  )
}
