import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table"
import { ArrowUp, ArrowDown, ArrowUpDown } from "lucide-react"
import { cn } from "@/lib/utils"

interface DataTableProps<TData> {
  columns: ColumnDef<TData, unknown>[]
  data: TData[]
  onRowClick?: (row: TData) => void
  sortColumn?: string
  sortDirection?: "ASC" | "DESC"
  onSort?: (column: string) => void
  sortableColumns?: string[]
  ariaLabel?: string
}

export function DataTable<TData>({
  columns,
  data,
  onRowClick,
  sortColumn,
  sortDirection,
  onSort,
  sortableColumns,
  ariaLabel,
}: DataTableProps<TData>) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <div className="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
      <div className="overflow-x-auto">
        <table
          aria-label={ariaLabel}
          className="min-w-full divide-y divide-gray-200 dark:divide-gray-700"
        >
          <thead className="bg-gray-50 dark:bg-gray-800">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const colId = header.column.id
                  const isSortable = onSort && sortableColumns?.includes(colId)
                  const isActive = sortColumn === colId
                  const ariaSortValue = isSortable && isActive
                    ? sortDirection === "ASC" ? "ascending" : "descending"
                    : isSortable ? "none" : undefined

                  return (
                    <th
                      key={header.id}
                      scope="col"
                      aria-sort={ariaSortValue}
                      onClick={isSortable ? () => onSort(colId) : undefined}
                      onKeyDown={isSortable ? (e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault()
                          onSort(colId)
                        }
                      } : undefined}
                      tabIndex={isSortable ? 0 : undefined}
                      role={isSortable ? "button" : undefined}
                      className={cn(
                        "px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400",
                        isSortable && "cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-300",
                      )}
                    >
                      <div className="flex items-center gap-1">
                        {header.isPlaceholder
                          ? null
                          : flexRender(header.column.columnDef.header, header.getContext())}
                        {isSortable && (
                          isActive ? (
                            sortDirection === "ASC" ? (
                              <ArrowUp className="h-3.5 w-3.5" />
                            ) : (
                              <ArrowDown className="h-3.5 w-3.5" />
                            )
                          ) : (
                            <ArrowUpDown className="h-3.5 w-3.5 text-gray-300 dark:text-gray-600" />
                          )
                        )}
                      </div>
                    </th>
                  )
                })}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700 bg-white dark:bg-gray-900">
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  className="px-4 py-8 text-center text-sm text-gray-500 dark:text-gray-400"
                >
                  No data
                </td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  onClick={() => onRowClick?.(row.original)}
                  onKeyDown={onRowClick ? (e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault()
                      onRowClick(row.original)
                    }
                  } : undefined}
                  tabIndex={onRowClick ? 0 : undefined}
                  role={onRowClick ? "link" : undefined}
                  className={cn(
                    onRowClick && "cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800 focus:outline-none focus:bg-gray-50 dark:focus:bg-gray-800",
                  )}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="whitespace-nowrap px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
