// Pure helpers for kanban reordering, extracted so the index arithmetic is
// unit-testable. Both bugs these guard against were live: an up/down click could
// silently do nothing, and the optimistic board could disagree with what was
// actually POSTed.

// Compute the new full-column order when a visible card is moved one step.
//
// `visibleColumnTasks` is what the operator can actually see (the buttons are
// indexed against it, and their disabled bounds come from its length), while
// `allTasks` is the unfiltered state. Applying a filtered-space step of +/-1
// directly to the unfiltered array made the move a no-op whenever a hidden card
// sat between two visible ones -- which the default view hits routinely via the
// 7-day done retention, plus domain and agent filters.
//
// Returns null when the move is not possible (already at an end, or the card is
// missing), so callers can bail without mutating anything.
export function computeReorderedColumn(allTasks, visibleColumnTasks, task, direction) {
    const status = task.task_status;
    const visible = Array.isArray(visibleColumnTasks) && visibleColumnTasks.length
        ? visibleColumnTasks
        : allTasks.filter(candidate => candidate.task_status === status);

    const visibleFrom = visible.findIndex(candidate => candidate.memory_id === task.memory_id);
    const visibleTo = visibleFrom + direction;
    if (visibleFrom < 0 || visibleTo < 0 || visibleTo >= visible.length) return null;
    const neighbourID = visible[visibleTo].memory_id;

    // Swap the two VISIBLE cards inside the full column ordering, leaving any
    // hidden cards between them exactly where they are.
    const columnTasks = allTasks.filter(candidate => candidate.task_status === status);
    const from = columnTasks.findIndex(candidate => candidate.memory_id === task.memory_id);
    const to = columnTasks.findIndex(candidate => candidate.memory_id === neighbourID);
    if (from < 0 || to < 0) return null;
    [columnTasks[from], columnTasks[to]] = [columnTasks[to], columnTasks[from]];
    return columnTasks.map(candidate => candidate.memory_id);
}

// Apply a new column order to the full task list optimistically.
//
// This deliberately does NOT sort. The previous implementation sorted the whole
// array with a comparator that returned 0 for any pair involving another
// column, which is non-transitive: V8's sort is free to leave such elements
// unmoved, so the board could silently keep the old order while the new one had
// already been POSTed. It only worked while every column happened to be
// contiguous in `allTasks` -- and every optimistic status rewrite (assign, move,
// clear-column) breaks that contiguity without relocating the element.
//
// Instead, rewrite only the target column's SLOTS, in the given order, leaving
// every other card at its exact index.
export function applyColumnOrder(allTasks, status, orderedIDs) {
    const byID = new Map(allTasks.map(task => [task.memory_id, task]));
    const ordered = orderedIDs.map(id => byID.get(id)).filter(Boolean);
    let next = 0;
    return allTasks.map(task =>
        task.task_status === status && next < ordered.length ? ordered[next++] : task);
}
