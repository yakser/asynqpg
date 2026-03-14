import { ChevronLeft, ChevronRight } from "lucide-react"

const PAGE_SIZES = [25, 50, 100]

interface PaginationProps {
  offset: number
  limit: number
  total: number
  onPageChange: (offset: number) => void
  onPageSizeChange?: (limit: number) => void
}

export function Pagination({ offset, limit, total, onPageChange, onPageSizeChange }: PaginationProps) {
  const currentPage = Math.floor(offset / limit) + 1
  const totalPages = Math.max(1, Math.ceil(total / limit))
  const hasPrev = offset > 0
  const hasNext = offset + limit < total

  return (
    <nav aria-label="Pagination" className="flex flex-col gap-3 px-2 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-center gap-3">
        <p className="hidden sm:block text-sm text-gray-500 dark:text-gray-400">
          Showing {total === 0 ? 0 : offset + 1}–{Math.min(offset + limit, total)} of {total}
        </p>
        {onPageSizeChange && (
          <select
            aria-label="Tasks per page"
            value={limit}
            onChange={(e) => onPageSizeChange(Number(e.target.value))}
            className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-sm text-gray-700 dark:text-gray-300"
          >
            {PAGE_SIZES.map((size) => (
              <option key={size} value={size}>
                {size} / page
              </option>
            ))}
          </select>
        )}
      </div>
      <div className="flex items-center gap-2">
        <button
          aria-label="Previous page"
          onClick={() => onPageChange(Math.max(0, offset - limit))}
          disabled={!hasPrev}
          className="inline-flex items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 p-2 min-h-[44px] min-w-[44px] sm:px-2 sm:py-1 sm:min-h-0 sm:min-w-0 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
        <span className="text-sm text-gray-700 dark:text-gray-300">
          Page {currentPage} of {totalPages}
        </span>
        <button
          aria-label="Next page"
          onClick={() => onPageChange(offset + limit)}
          disabled={!hasNext}
          className="inline-flex items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 p-2 min-h-[44px] min-w-[44px] sm:px-2 sm:py-1 sm:min-h-0 sm:min-w-0 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </nav>
  )
}
