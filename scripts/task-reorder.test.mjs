import assert from 'node:assert/strict';
import test from 'node:test';

const { computeReorderedColumn, applyColumnOrder } = await import(
    '../web/static/js/task-reorder.js'
);

const task = (id, status) => ({ memory_id: id, task_status: status });
const ids = (list) => list.map((entry) => entry.memory_id);

test('a hidden card between two visible ones does not absorb the move', () => {
    // P2 is hidden (filtered out); the operator sees P1 above P3 and clicks up on P3.
    const all = [task('P1', 'planned'), task('P2', 'planned'), task('P3', 'planned')];
    const visible = [all[0], all[2]];

    const ordered = computeReorderedColumn(all, visible, all[2], -1);
    assert.deepEqual(ordered, ['P3', 'P2', 'P1'],
        'the two VISIBLE cards must swap; the hidden card stays between them');

    // Regression: indexing the step into the unfiltered array would have swapped
    // P3 with the hidden P2, which the operator sees as nothing happening.
    assert.notDeepEqual(ordered, ['P1', 'P3', 'P2']);
});

test('bounds come from the visible column, not the full one', () => {
    const all = [task('P1', 'planned'), task('P2', 'planned'), task('P3', 'planned')];
    const visible = [all[1]]; // only one card visible

    assert.equal(computeReorderedColumn(all, visible, all[1], -1), null);
    assert.equal(computeReorderedColumn(all, visible, all[1], 1), null);
});

test('an ordinary adjacent move still works', () => {
    const all = [task('A', 'planned'), task('B', 'planned'), task('C', 'planned')];
    assert.deepEqual(computeReorderedColumn(all, all, all[0], 1), ['B', 'A', 'C']);
    assert.deepEqual(computeReorderedColumn(all, all, all[2], -1), ['A', 'C', 'B']);
});

test('applying an order leaves other columns untouched and non-contiguous columns correct', () => {
    // The exact shape an optimistic status rewrite produces: the in_progress
    // cards are NOT contiguous, because P2 was reassigned in place.
    const all = [
        task('I1', 'in_progress'),
        task('I2', 'in_progress'),
        task('P1', 'planned'),
        task('P2', 'in_progress'),
        task('P3', 'planned'),
        task('D1', 'done'),
    ];

    const next = applyColumnOrder(all, 'in_progress', ['I1', 'P2', 'I2']);

    // in_progress slots (indices 0, 1, 3) now hold the new order.
    assert.deepEqual(ids(next), ['I1', 'P2', 'P1', 'I2', 'P3', 'D1']);
    // Reading the column back gives exactly what was POSTed.
    assert.deepEqual(
        ids(next.filter((entry) => entry.task_status === 'in_progress')),
        ['I1', 'P2', 'I2'],
    );
    // Other columns are untouched, in both position and order.
    assert.deepEqual(ids(next.filter((entry) => entry.task_status === 'planned')), ['P1', 'P3']);
    assert.deepEqual(ids(next.filter((entry) => entry.task_status === 'done')), ['D1']);
});

test('the old non-transitive comparator would have failed this case', () => {
    // Kept as an executable record of the defect: sorting the whole array with a
    // comparator that returns 0 across columns leaves out-of-run elements in
    // place, so the board silently disagreed with what was POSTed.
    const all = [
        task('I1', 'in_progress'),
        task('I2', 'in_progress'),
        task('P1', 'planned'),
        task('P2', 'in_progress'),
        task('P3', 'planned'),
    ];
    const orderedIDs = ['I1', 'P2', 'I2'];
    const rank = new Map(orderedIDs.map((id, index) => [id, index]));

    const oldWay = ids([...all].sort((a, b) => {
        if (a.task_status !== 'in_progress' || b.task_status !== 'in_progress') return 0;
        return rank.get(a.memory_id) - rank.get(b.memory_id);
    }).filter((entry) => entry.task_status === 'in_progress'));

    const newWay = ids(
        applyColumnOrder(all, 'in_progress', orderedIDs)
            .filter((entry) => entry.task_status === 'in_progress'),
    );

    assert.deepEqual(newWay, orderedIDs, 'the fix must reproduce exactly what was POSTed');
    assert.notDeepEqual(oldWay, orderedIDs,
        'if this ever matches, the old comparator was not actually broken here');
});
