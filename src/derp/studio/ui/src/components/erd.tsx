import { Key, Link } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { TableInfo } from "../api";

// --- Layout constants ---

const TABLE_WIDTH = 220;
const HEADER_HEIGHT = 32;
const ROW_HEIGHT = 24;
const TABLE_GAP_X = 80;
const TABLE_GAP_Y = 60;
const PADDING = 40;

// --- Types ---

interface TableNode {
  table: TableInfo;
  x: number;
  y: number;
  width: number;
  height: number;
}

interface Edge {
  from: { table: string; column: string };
  to: { table: string; column: string };
  onDelete: string | null;
}

// --- Layout ---

function layoutTables(tables: TableInfo[]): TableNode[] {
  if (tables.length === 0) return [];

  // Build adjacency from foreign keys to place connected tables closer
  const connections = new Map<string, Set<string>>();
  for (const t of tables) {
    connections.set(t.name, new Set());
  }
  for (const t of tables) {
    for (const fk of t.foreign_keys) {
      connections.get(t.name)?.add(fk.references_table);
      connections.get(fk.references_table)?.add(t.name);
    }
  }

  // Sort: tables with more connections first for better layout
  const sorted = [...tables].sort((a, b) => {
    const ac = connections.get(a.name)?.size ?? 0;
    const bc = connections.get(b.name)?.size ?? 0;
    return bc - ac;
  });

  // Simple grid layout
  const cols = Math.max(1, Math.ceil(Math.sqrt(sorted.length)));

  const nodes: TableNode[] = [];
  for (let i = 0; i < sorted.length; i++) {
    const t = sorted[i];
    const col = i % cols;
    const row = Math.floor(i / cols);
    const height = HEADER_HEIGHT + t.columns.length * ROW_HEIGHT + 8;
    nodes.push({
      table: t,
      x: PADDING + col * (TABLE_WIDTH + TABLE_GAP_X),
      y: PADDING + row * (Math.max(...sorted.map((s) => HEADER_HEIGHT + s.columns.length * ROW_HEIGHT + 8)) + TABLE_GAP_Y),
      width: TABLE_WIDTH,
      height,
    });
  }

  return nodes;
}

function buildEdges(tables: TableInfo[]): Edge[] {
  const edges: Edge[] = [];
  for (const t of tables) {
    for (const fk of t.foreign_keys) {
      edges.push({
        from: { table: t.name, column: fk.columns[0] },
        to: { table: fk.references_table, column: fk.references_columns[0] },
        onDelete: fk.on_delete,
      });
    }
  }
  return edges;
}

// --- Edge colors ---

const EDGE_COLORS = [
  "#6366f1", // indigo
  "#f59e0b", // amber
  "#10b981", // emerald
  "#ef4444", // red
  "#8b5cf6", // violet
  "#06b6d4", // cyan
  "#f97316", // orange
  "#ec4899", // pink
];

// --- SVG rendering ---

function TableBox({
  node,
  edges,
  onTableClick,
}: {
  node: TableNode;
  edges: Edge[];
  onTableClick?: (name: string) => void;
}) {
  const { table, x, y, width } = node;

  // Columns that are FKs
  const fkColumns = new Set(
    table.foreign_keys.flatMap((fk) => fk.columns),
  );

  // Columns that are referenced by other tables
  const referencedColumns = new Set<string>();
  for (const e of edges) {
    if (e.to.table === table.name) {
      referencedColumns.add(e.to.column);
    }
  }

  return (
    <g
      className="cursor-pointer"
      onClick={() => onTableClick?.(table.name)}
    >
      {/* Shadow */}
      <rect
        x={x + 2}
        y={y + 2}
        width={width}
        height={node.height}
        rx={6}
        fill="rgba(0,0,0,0.08)"
      />
      {/* Background */}
      <rect
        x={x}
        y={y}
        width={width}
        height={node.height}
        rx={6}
        fill="var(--color-card, #fff)"
        stroke="var(--color-border, #e5e7eb)"
        strokeWidth={1.5}
      />
      {/* Header */}
      <rect
        x={x}
        y={y}
        width={width}
        height={HEADER_HEIGHT}
        rx={6}
        fill="var(--color-primary, #18181b)"
      />
      {/* Cover bottom corners of header */}
      <rect
        x={x}
        y={y + HEADER_HEIGHT - 6}
        width={width}
        height={6}
        fill="var(--color-primary, #18181b)"
      />
      <text
        x={x + 12}
        y={y + HEADER_HEIGHT / 2}
        dominantBaseline="central"
        fill="var(--color-primary-foreground, #fff)"
        fontSize={13}
        fontWeight={600}
        fontFamily="ui-monospace, SFMono-Regular, monospace"
      >
        {table.name}
      </text>

      {/* Columns */}
      {table.columns.map((col, i) => {
        const cy = y + HEADER_HEIGHT + i * ROW_HEIGHT + ROW_HEIGHT / 2 + 4;
        const isPK = col.primary_key;
        const isFK = fkColumns.has(col.name);
        const isReferenced = referencedColumns.has(col.name);

        return (
          <g key={col.name}>
            {/* Hover highlight */}
            <rect
              x={x + 1}
              y={cy - ROW_HEIGHT / 2}
              width={width - 2}
              height={ROW_HEIGHT}
              fill="transparent"
              className="hover:fill-(--color-muted,#f4f4f5)"
              rx={2}
            />
            {/* Icons */}
            {isPK ? (
              <g transform={`translate(${x + 10}, ${cy - 6})`}>
                <Key size={12} className="text-amber-500" />
              </g>
            ) : null}
            {(isFK || isReferenced) && !isPK ? (
              <g transform={`translate(${x + 10}, ${cy - 6})`}>
                <Link size={12} className="text-blue-400" />
              </g>
            ) : null}
            {/* Column name */}
            <text
              x={x + 28}
              y={cy}
              dominantBaseline="central"
              fontSize={12}
              fontFamily="ui-monospace, SFMono-Regular, monospace"
              fill={
                isPK
                  ? "var(--color-foreground, #18181b)"
                  : "var(--color-muted-foreground, #71717a)"
              }
              fontWeight={isPK ? 600 : 400}
            >
              {col.name}
            </text>
            {/* Column type */}
            <text
              x={x + width - 10}
              y={cy}
              dominantBaseline="central"
              textAnchor="end"
              fontSize={10}
              fontFamily="ui-monospace, SFMono-Regular, monospace"
              fill="var(--color-muted-foreground, #a1a1aa)"
            >
              {col.type}
            </text>
          </g>
        );
      })}
    </g>
  );
}

function getColumnY(node: TableNode, columnName: string): number {
  const idx = node.table.columns.findIndex((c) => c.name === columnName);
  if (idx === -1) return node.y + HEADER_HEIGHT + 16;
  return node.y + HEADER_HEIGHT + idx * ROW_HEIGHT + ROW_HEIGHT / 2 + 4;
}

function EdgeLine({
  edge,
  fromNode,
  toNode,
  color,
}: {
  edge: Edge;
  fromNode: TableNode;
  toNode: TableNode;
  color: string;
}) {
  const fromY = getColumnY(fromNode, edge.from.column);
  const toY = getColumnY(toNode, edge.to.column);

  // Determine which sides to connect
  const fromCenterX = fromNode.x + fromNode.width / 2;
  const toCenterX = toNode.x + toNode.width / 2;

  let fromX: number;
  let toX: number;

  if (fromCenterX < toCenterX) {
    fromX = fromNode.x + fromNode.width;
    toX = toNode.x;
  } else if (fromCenterX > toCenterX) {
    fromX = fromNode.x;
    toX = toNode.x + toNode.width;
  } else {
    // Same column - connect via the right side
    fromX = fromNode.x + fromNode.width;
    toX = toNode.x + toNode.width;
  }

  const dx = Math.abs(toX - fromX);
  const cpOffset = Math.max(40, dx * 0.4);

  const cp1x = fromX + (fromX < toX ? cpOffset : -cpOffset);
  const cp2x = toX + (toX < fromX ? cpOffset : -cpOffset);

  const path = `M ${fromX} ${fromY} C ${cp1x} ${fromY}, ${cp2x} ${toY}, ${toX} ${toY}`;

  return (
    <g>
      <path
        d={path}
        fill="none"
        stroke={color}
        strokeWidth={2}
        strokeOpacity={0.6}
        markerEnd={`url(#arrow-${color.replace("#", "")})`}
      />
      {/* FK dot at source */}
      <circle cx={fromX} cy={fromY} r={3.5} fill={color} fillOpacity={0.8} />
    </g>
  );
}

// --- Main component ---

export function ERDiagram({
  tables,
  onTableClick,
}: {
  tables: TableInfo[];
  onTableClick?: (name: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1200, h: 800 });
  const [dragging, setDragging] = useState(false);
  const dragStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 });

  const nodes = useMemo(() => layoutTables(tables), [tables]);
  const edges = useMemo(() => buildEdges(tables), [tables]);

  // Compute SVG bounds
  const bounds = useMemo(() => {
    if (nodes.length === 0) return { w: 1200, h: 800 };
    let maxX = 0;
    let maxY = 0;
    for (const n of nodes) {
      maxX = Math.max(maxX, n.x + n.width + PADDING);
      maxY = Math.max(maxY, n.y + n.height + PADDING);
    }
    return { w: maxX, h: maxY };
  }, [nodes]);

  // Fit to view on first render
  useEffect(() => {
    setViewBox({ x: 0, y: 0, w: bounds.w, h: bounds.h });
  }, [bounds]);

  const nodeMap = useMemo(() => {
    const m = new Map<string, TableNode>();
    for (const n of nodes) m.set(n.table.name, n);
    return m;
  }, [nodes]);

  // Unique edge colors
  const edgeColors = useMemo(() => {
    return edges.map((_, i) => EDGE_COLORS[i % EDGE_COLORS.length]);
  }, [edges]);

  // Pan handlers
  const onPointerDown = useCallback(
    (e: React.PointerEvent) => {
      if (e.button !== 0) return;
      setDragging(true);
      dragStart.current = {
        x: e.clientX,
        y: e.clientY,
        vx: viewBox.x,
        vy: viewBox.y,
      };
      (e.target as HTMLElement).setPointerCapture?.(e.pointerId);
    },
    [viewBox],
  );

  const onPointerMove = useCallback(
    (e: React.PointerEvent) => {
      if (!dragging) return;
      const container = containerRef.current;
      if (!container) return;
      const rect = container.getBoundingClientRect();
      const scaleX = viewBox.w / rect.width;
      const scaleY = viewBox.h / rect.height;
      const dx = (e.clientX - dragStart.current.x) * scaleX;
      const dy = (e.clientY - dragStart.current.y) * scaleY;
      setViewBox((prev) => ({
        ...prev,
        x: dragStart.current.vx - dx,
        y: dragStart.current.vy - dy,
      }));
    },
    [dragging, viewBox.w, viewBox.h],
  );

  const onPointerUp = useCallback(() => {
    setDragging(false);
  }, []);

  // Zoom handler
  const onWheel = useCallback(
    (e: React.WheelEvent) => {
      e.preventDefault();
      const container = containerRef.current;
      if (!container) return;
      const rect = container.getBoundingClientRect();

      const factor = e.deltaY > 0 ? 1.08 : 1 / 1.08;

      // Mouse position in viewBox coords
      const mx = viewBox.x + ((e.clientX - rect.left) / rect.width) * viewBox.w;
      const my = viewBox.y + ((e.clientY - rect.top) / rect.height) * viewBox.h;

      const newW = viewBox.w * factor;
      const newH = viewBox.h * factor;
      const newX = mx - (mx - viewBox.x) * factor;
      const newY = my - (my - viewBox.y) * factor;

      setViewBox({ x: newX, y: newY, w: newW, h: newH });
    },
    [viewBox],
  );

  if (tables.length === 0) {
    return (
      <div className="flex items-center justify-center py-32 text-sm text-muted-foreground">
        No tables found.
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={`w-full h-full min-h-[500px] rounded-lg border bg-muted/20 overflow-hidden ${dragging ? "cursor-grabbing" : "cursor-grab"}`}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerLeave={onPointerUp}
      onWheel={onWheel}
    >
      <svg
        width="100%"
        height="100%"
        viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.w} ${viewBox.h}`}
        className="select-none"
      >
        <defs>
          {EDGE_COLORS.map((color) => (
            <marker
              key={color}
              id={`arrow-${color.replace("#", "")}`}
              viewBox="0 0 10 8"
              refX={10}
              refY={4}
              markerWidth={8}
              markerHeight={6}
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 4 L 0 8 z" fill={color} fillOpacity={0.7} />
            </marker>
          ))}
        </defs>

        {/* Edges */}
        {edges.map((edge, i) => {
          const fromNode = nodeMap.get(edge.from.table);
          const toNode = nodeMap.get(edge.to.table);
          if (!fromNode || !toNode) return null;
          return (
            <EdgeLine
              key={`${edge.from.table}-${edge.from.column}-${edge.to.table}-${edge.to.column}`}
              edge={edge}
              fromNode={fromNode}
              toNode={toNode}
              color={edgeColors[i]}
            />
          );
        })}

        {/* Table boxes */}
        {nodes.map((node) => (
          <TableBox
            key={node.table.name}
            node={node}
            edges={edges}
            onTableClick={onTableClick}
          />
        ))}
      </svg>
    </div>
  );
}
