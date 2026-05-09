import { Icon } from "./Icon"

interface FilterFieldProps {
  value: string
  onChange: (v: string) => void
  placeholder?: string
  count?: number
}

export function FilterField({ value, onChange, placeholder, count }: FilterFieldProps) {
  return (
    <label className="filter-field">
      <Icon name="search" size={14} />
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        spellCheck={false}
      />
      {value && (
        <button
          type="button"
          className="filter-clear"
          onClick={() => onChange("")}
          aria-label="Clear"
        >
          <Icon name="x" size={12} />
        </button>
      )}
      {!value && count != null && <span className="filter-count">{count}</span>}
    </label>
  )
}
