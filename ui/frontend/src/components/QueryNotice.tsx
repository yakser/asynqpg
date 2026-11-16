interface QueryNoticeProps {
  label: string
  isPending: boolean
  isError: boolean
  hasData: boolean
  onRetry: () => void
}

export function QueryNotice({ label, isPending, isError, hasData, onRetry }: QueryNoticeProps): React.JSX.Element | null {
  if (isError) {
    return (
      <div className="query-notice" role="alert">
        <span>{hasData ? `Could not refresh ${label}. Displayed data is stale.` : `Could not load ${label}.`}</span>
        <button type="button" className="btn" onClick={onRetry}>Retry</button>
      </div>
    )
  }
  if (isPending) return <div className="query-notice" role="status">Loading {label}…</div>
  return null
}
