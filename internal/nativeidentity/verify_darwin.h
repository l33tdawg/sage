#ifndef SAGE_NATIVE_IDENTITY_H
#define SAGE_NATIVE_IDENTITY_H

#include <stdint.h>
#include <mach/message.h>

typedef audit_token_t sage_peer_token;

typedef struct {
    char identifier[1024];
    char team_id[256];
    unsigned char cdhash[64];
    unsigned int cdhash_len;
    uint32_t uid;
    int32_t pid;
} sage_peer_identity;

enum {
    SAGE_PEER_OK = 0,
    SAGE_PEER_REQUIREMENT,
    SAGE_PEER_UID,
    SAGE_PEER_LOOKUP,
    SAGE_PEER_VALIDITY,
    SAGE_PEER_INFORMATION,
    SAGE_PEER_RUNTIME,
    SAGE_PEER_ENTITLEMENT
};

int sage_peer_token_read(int fd, sage_peer_token *token);
int sage_peer_verify(const sage_peer_token *token, const char *requirement,
                     sage_peer_identity *identity, int32_t *detail);
const char *sage_peer_stage_name(int stage);

#endif
