//go:build darwin && cgo

#include "verify_darwin.h"
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <bsm/libbsm.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

// LOCAL_PEERTOKEN returns the peer task's current kernel audit token. Never
// substitute LOCAL_PEERPID + a later user-space PID lookup. This API does not
// promise an immutable connect-time identity; Verify also checks it afterward.
int sage_peer_token_read(int fd, sage_peer_token *token) {
    struct sockaddr_storage address;
    socklen_t address_len = sizeof(address);
    int type = 0;
    socklen_t type_len = sizeof(type);
    memset(token, 0, sizeof(*token));
    if (getpeername(fd, (struct sockaddr *)&address, &address_len) != 0) return errno;
    if (address.ss_family != AF_UNIX) return EPROTOTYPE;
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &type_len) != 0) return errno;
    if (type_len != sizeof(type) || type != SOCK_STREAM) return EPROTOTYPE;
    socklen_t token_len = sizeof(*token);
    if (getsockopt(fd, SOL_LOCAL, LOCAL_PEERTOKEN, token, &token_len) != 0) return errno;
    if (token_len != sizeof(*token)) return EINVAL;
    return 0;
}

static int copy_string(CFDictionaryRef info, CFStringRef key, char *output,
                       CFIndex size, int optional) {
    CFTypeRef value = CFDictionaryGetValue(info, key);
    if (value == NULL) return optional;
    if (CFGetTypeID(value) != CFStringGetTypeID()) return 0;
    return CFStringGetCString((CFStringRef)value, output, size, kCFStringEncodingUTF8)
        && (optional || output[0] != '\0');
}

static int copy_number(CFDictionaryRef info, CFStringRef key, int64_t *output) {
    CFTypeRef value = CFDictionaryGetValue(info, key);
    return value != NULL && CFGetTypeID(value) == CFNumberGetTypeID()
        && CFNumberGetValue((CFNumberRef)value, kCFNumberSInt64Type, output);
}

// Reject enabled or malformed relaxation entitlements. An absent or explicitly
// false Boolean leaves the corresponding hardened-runtime protection in place.
static int no_relaxed_runtime(CFDictionaryRef info) {
    CFTypeRef entitlements = CFDictionaryGetValue(info, kSecCodeInfoEntitlementsDict);
    if (entitlements == NULL) return 1;
    if (CFGetTypeID(entitlements) != CFDictionaryGetTypeID()) return 0;
    const CFStringRef denied[] = {
        CFSTR("com.apple.security.get-task-allow"),
        CFSTR("get-task-allow"),
        CFSTR("com.apple.security.cs.allow-jit"),
        CFSTR("com.apple.security.cs.allow-unsigned-executable-memory"),
        CFSTR("com.apple.security.cs.allow-dyld-environment-variables"),
        CFSTR("com.apple.security.cs.disable-library-validation"),
        CFSTR("com.apple.security.cs.disable-executable-page-protection")
    };
    for (unsigned int i = 0; i < sizeof(denied) / sizeof(denied[0]); i++) {
        CFTypeRef value = CFDictionaryGetValue((CFDictionaryRef)entitlements, denied[i]);
        if (value != NULL && (CFGetTypeID(value) != CFBooleanGetTypeID()
                            || CFBooleanGetValue((CFBooleanRef)value))) return 0;
    }
    return 1;
}

int sage_peer_verify(const sage_peer_token *token, const char *requirement,
                     sage_peer_identity *identity, int32_t *detail) {
    int stage = SAGE_PEER_INFORMATION;
    OSStatus status = errSecSuccess;
    CFStringRef policy_text = NULL;
    SecRequirementRef policy = NULL;
    CFDataRef audit = NULL;
    CFDictionaryRef attributes = NULL;
    SecCodeRef code = NULL;
    CFDictionaryRef info = NULL;
    memset(identity, 0, sizeof(*identity));
    *detail = 0;

    if (audit_token_to_euid(*token) != geteuid()) return SAGE_PEER_UID;

    stage = SAGE_PEER_REQUIREMENT;
    policy_text = CFStringCreateWithCString(NULL, requirement, kCFStringEncodingUTF8);
    if (policy_text == NULL) goto cleanup;
    status = SecRequirementCreateWithString(policy_text, kSecCSDefaultFlags, &policy);
    if (status != errSecSuccess || policy == NULL) goto cleanup;

    stage = SAGE_PEER_LOOKUP;
    audit = CFDataCreate(NULL, (const UInt8 *)token, sizeof(*token));
    if (audit == NULL) goto cleanup;
    const void *keys[] = { kSecGuestAttributeAudit };
    const void *values[] = { audit };
    attributes = CFDictionaryCreate(NULL, keys, values, 1,
                                   &kCFTypeDictionaryKeyCallBacks,
                                   &kCFTypeDictionaryValueCallBacks);
    if (attributes == NULL) goto cleanup;
    // The kernel authenticates the complete audit token, including pidversion.
    status = SecCodeCopyGuestWithAttributes(NULL, attributes, kSecCSDefaultFlags, &code);
    if (status != errSecSuccess || code == NULL) goto cleanup;

    stage = SAGE_PEER_VALIDITY;
    // This is dynamic verification of the actual process, not a path-based static
    // check. The API also validates the backing identity-sealed components.
    status = SecCodeCheckValidity(code, kSecCSStrictValidate | kSecCSNoNetworkAccess, policy);
    if (status != errSecSuccess) goto cleanup;

    stage = SAGE_PEER_INFORMATION;
    status = SecCodeCopySigningInformation(code, kSecCSSigningInformation | kSecCSDynamicInformation, &info);
    if (status != errSecSuccess || info == NULL) goto cleanup;
    if (!copy_string(info, kSecCodeInfoIdentifier, identity->identifier, sizeof(identity->identifier), 0)
        || !copy_string(info, kSecCodeInfoTeamIdentifier, identity->team_id, sizeof(identity->team_id), 1)) goto cleanup;
    CFTypeRef hash = CFDictionaryGetValue(info, kSecCodeInfoUnique);
    if (hash == NULL || CFGetTypeID(hash) != CFDataGetTypeID()) goto cleanup;
    CFIndex hash_len = CFDataGetLength((CFDataRef)hash);
    if (hash_len <= 0 || hash_len > sizeof(identity->cdhash)) goto cleanup;
    CFDataGetBytes((CFDataRef)hash, CFRangeMake(0, hash_len), identity->cdhash);
    identity->cdhash_len = (unsigned int)hash_len;

    stage = SAGE_PEER_RUNTIME;
    int64_t flags = 0, dynamic_status = 0;
    if (!copy_number(info, kSecCodeInfoFlags, &flags)
        || !copy_number(info, kSecCodeInfoStatus, &dynamic_status)
        || !(flags & kSecCodeSignatureRuntime)
        || !(dynamic_status & kSecCodeStatusValid)
        || (dynamic_status & kSecCodeStatusDebugged)) goto cleanup;

    stage = SAGE_PEER_ENTITLEMENT;
    if (!no_relaxed_runtime(info)) goto cleanup;

    // Recheck dynamic validity after extracting metadata. This narrows, but does
    // not claim to eliminate, the inherent point-in-time process-lifetime window.
    stage = SAGE_PEER_VALIDITY;
    status = SecCodeCheckValidity(code, kSecCSStrictValidate | kSecCSNoNetworkAccess, policy);
    if (status != errSecSuccess) goto cleanup;
    identity->uid = audit_token_to_euid(*token);
    identity->pid = audit_token_to_pid(*token);
    stage = SAGE_PEER_OK;

cleanup:
    *detail = status;
    if (info != NULL) CFRelease(info);
    if (code != NULL) CFRelease(code);
    if (attributes != NULL) CFRelease(attributes);
    if (audit != NULL) CFRelease(audit);
    if (policy != NULL) CFRelease(policy);
    if (policy_text != NULL) CFRelease(policy_text);
    if (stage != SAGE_PEER_OK) memset(identity, 0, sizeof(*identity));
    return stage;
}

const char *sage_peer_stage_name(int stage) {
    switch (stage) {
        case SAGE_PEER_UID: return "peer effective UID";
        case SAGE_PEER_LOOKUP: return "audit-token code lookup";
        case SAGE_PEER_VALIDITY: return "dynamic code validity";
        case SAGE_PEER_INFORMATION: return "verified signing information";
        case SAGE_PEER_RUNTIME: return "hardened runtime or debug state";
        case SAGE_PEER_ENTITLEMENT: return "runtime relaxation entitlement";
        default: return "peer verification";
    }
}
