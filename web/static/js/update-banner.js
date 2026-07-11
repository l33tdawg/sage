export function buildUpdateBanner(data, dismissedRelease = '') {
    if (!data || data.error || (!data.update_available && !data.restart_required)) return null;

    const release = String(data.disk_version || data.latest_version || 'available');
    if (dismissedRelease === release) return null;

    const version = String(data.disk_version || data.latest_version || '').replace(/^v/, '');
    const restartRequired = !!data.restart_required;
    const canInstall = !restartRequired
        && data.in_app_update_supported !== false
        && !!data.download_url
        && !!data.checksum;
    return {
        ...data,
        banner_can_install: canInstall,
        banner_release: release,
        banner_title: restartRequired
            ? `SAGE ${version || 'update'} is installed and ready to restart`
            : `SAGE ${version || 'update'} is available`,
        banner_message: restartRequired
            ? 'Restart SAGE to start using the new release.'
            : 'A newer release is ready when you are.',
        banner_action: restartRequired
            ? 'Restart options'
            : (canInstall ? 'Download & Install' : 'View update'),
    };
}
