import assert from 'node:assert/strict';
import test from 'node:test';

import { buildUpdateBanner } from '../web/static/js/update-banner.js';

test('update banner stays hidden when SAGE is current or the check fails', () => {
    assert.equal(buildUpdateBanner({ latest_version: '11.7.0', update_available: false }), null);
    assert.equal(buildUpdateBanner({ error: 'offline', update_available: true }), null);
});

test('update banner describes a newly available release', () => {
    const banner = buildUpdateBanner({
        latest_version: '11.8.0',
        update_available: true,
        release_url: 'https://example.com/release',
    });
    assert.equal(banner.banner_release, '11.8.0');
    assert.equal(banner.banner_title, 'SAGE 11.8.0 is available');
    assert.equal(banner.banner_action, 'View update');
    assert.equal(banner.banner_can_install, false);
});

test('update banner offers one-click install when the signed artifact is ready', () => {
    const banner = buildUpdateBanner({
        latest_version: '11.8.0',
        update_available: true,
        in_app_update_supported: true,
        download_url: 'https://example.com/SAGE.dmg',
        checksum: 'abc123',
    });
    assert.equal(banner.banner_action, 'Download & Install');
    assert.equal(banner.banner_can_install, true);
});

test('dismissal lasts for that release and a newer release appears again', () => {
    assert.equal(buildUpdateBanner({ latest_version: '11.8.0', update_available: true }, '11.8.0'), null);
    assert.ok(buildUpdateBanner({ latest_version: '11.8.1', update_available: true }, '11.8.0'));
});

test('update banner distinguishes an installed release awaiting restart', () => {
    const banner = buildUpdateBanner({
        latest_version: '11.8.0',
        disk_version: 'v11.8.0',
        update_available: false,
        restart_required: true,
    });
    assert.equal(banner.banner_release, 'v11.8.0');
    assert.equal(banner.banner_title, 'SAGE 11.8.0 is installed and ready to restart');
    assert.equal(banner.banner_action, 'Restart options');
});
