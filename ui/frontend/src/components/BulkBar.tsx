import { Button } from "./Button"
import { Icon } from "./Icon"

interface BulkBarProps {
  count: number
  onRetry: () => void
  onCancel: () => void
  onDelete: () => void
  onClear: () => void
}

export function BulkBar({
  count,
  onRetry,
  onCancel,
  onDelete,
  onClear,
}: BulkBarProps) {
  return (
    <div className="bulk-bar">
      <Icon name="check-square" size={14} style={{ color: "var(--brand-600)" }} />
      <span>
        <span className="ct">{count}</span> selected
      </span>
      <span style={{ flex: 1 }} />
      <Button icon="rotate-ccw" size="sm" onClick={onRetry}>
        Retry
      </Button>
      <Button icon="x-circle" size="sm" onClick={onCancel}>
        Cancel
      </Button>
      <Button icon="trash-2" size="sm" variant="danger" onClick={onDelete}>
        Delete
      </Button>
      <span style={{ width: 6 }} />
      <Button variant="ghost" size="sm" onClick={onClear}>
        Clear
      </Button>
    </div>
  )
}
