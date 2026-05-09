import {
  ArrowRight,
  Bell,
  BookOpen,
  BookmarkPlus,
  Check,
  CheckSquare,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Copy,
  Cpu,
  Crown,
  Download,
  ExternalLink,
  Filter,
  Hash,
  LayoutDashboard,
  List,
  type LucideIcon,
  Monitor,
  Moon,
  Play,
  Plus,
  RefreshCw,
  RotateCcw,
  RotateCw,
  Search,
  SearchX,
  Sun,
  Terminal,
  Trash2,
  User,
  Wrench,
  X,
  XCircle,
} from "lucide-react"

const ICONS: Record<string, LucideIcon> = {
  "arrow-right": ArrowRight,
  bell: Bell,
  "book-open": BookOpen,
  "bookmark-plus": BookmarkPlus,
  check: Check,
  "check-square": CheckSquare,
  "chevron-down": ChevronDown,
  "chevron-right": ChevronRight,
  "chevron-up": ChevronUp,
  copy: Copy,
  cpu: Cpu,
  crown: Crown,
  download: Download,
  "external-link": ExternalLink,
  filter: Filter,
  hash: Hash,
  "layout-dashboard": LayoutDashboard,
  list: List,
  monitor: Monitor,
  moon: Moon,
  play: Play,
  plus: Plus,
  "refresh-cw": RefreshCw,
  "rotate-ccw": RotateCcw,
  "rotate-cw": RotateCw,
  search: Search,
  "search-x": SearchX,
  sun: Sun,
  terminal: Terminal,
  "trash-2": Trash2,
  user: User,
  wrench: Wrench,
  x: X,
  "x-circle": XCircle,
}

export interface IconProps {
  name: string
  size?: number
  className?: string
  style?: React.CSSProperties
}

export function Icon({ name, size = 14, className, style }: IconProps) {
  const Comp = ICONS[name]
  if (!Comp) {
    return <span className={className} style={style} aria-hidden />
  }
  return <Comp size={size} className={className ? "ic " + className : "ic"} style={style} aria-hidden />
}
