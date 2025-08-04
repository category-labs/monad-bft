// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

/*
 * TODO:
 * - include more information in protocol title
 *   (similar to "User Datagram Protocol, Src Port: 8000, Dst Port: 8000")
 * - perform upper-level protocol decode of small messages
 * - somehow indicate grouping of packets belonging to the same app message
 * - attempt Raptor decode of multi-packet messages
 */

#define WS_BUILD_DLL
#include <wireshark.h>
#if (WIRESHARK_VERSION_MAJOR == 4 && WIRESHARK_VERSION_MINOR >= 3) || WIRESHARK_VERSION_MAJOR > 4
#include <wsutil/plugins.h>
#endif
#include <epan/packet.h>
#include <blake3.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>

#ifndef VERSION
#define VERSION "0.0.1"
#endif

WS_DLL_PUBLIC_DEF const char plugin_version[] = VERSION;
WS_DLL_PUBLIC_DEF const int plugin_want_major = WIRESHARK_VERSION_MAJOR;
WS_DLL_PUBLIC_DEF const int plugin_want_minor = WIRESHARK_VERSION_MINOR;

WS_DLL_PUBLIC void plugin_register(void);
#if (WIRESHARK_VERSION_MAJOR == 4 && WIRESHARK_VERSION_MINOR >= 3) || WIRESHARK_VERSION_MAJOR > 4
WS_DLL_PUBLIC uint32_t plugin_describe(void);
#endif

#if WIRESHARK_VERSION_MAJOR < 4 || (WIRESHARK_VERSION_MAJOR == 4 && WIRESHARK_VERSION_MINOR < 3)
#define tvb_get_uint8	tvb_get_gint8
#endif

// The default RaptorCast UDP port number.
#define RAPTORCAST_PORT		8000

// The commit timestamp for the most recent incompatible format change.
#define RAPTORCAST_MIN_TIMESTAMP	((uint64_t)1725030240000)

struct raptorcast_header {
	struct raptorcast_signature {
		uint8_t		r[32];
		uint8_t		s[32];
		uint8_t		v;
	} signature;
	uint16_t	version;
	uint8_t		flags;
	uint64_t	epoch_no;
	uint64_t	unix_ts_ms;
	uint8_t		app_message_hash[20];
	uint32_t	app_message_len;
} __attribute__((packed));

struct raptorcast_merkle_item {
	uint8_t		hash[20];
};

struct raptorcast_chunk_header {
	uint8_t		first_hop_recipient[20];
	uint8_t		merkle_leaf_index;
	uint8_t		reserved;
	uint16_t	encoding_symbol_id;
};

#define sizeof_member(TYPE, MEMBER)	sizeof(((TYPE *)0)->MEMBER)

#if WIRESHARK_VERSION_MAJOR < 4 || (WIRESHARK_VERSION_MAJOR == 4 && WIRESHARK_VERSION_MINOR < 3)
#define INIT_VAL	-1
#else
#define INIT_VAL	0
#endif

static int proto_raptorcast = INIT_VAL;

static int hf_raptorcast_author = INIT_VAL;
static int hf_raptorcast_signature_r = INIT_VAL;
static int hf_raptorcast_signature_s = INIT_VAL;
static int hf_raptorcast_signature_v = INIT_VAL;
static int hf_raptorcast_signature_valid = INIT_VAL;
static int hf_raptorcast_version = INIT_VAL;
static int hf_raptorcast_flags_broadcast = INIT_VAL;
static int hf_raptorcast_flags_merkle_tree_depth = INIT_VAL;
static int hf_raptorcast_epoch = INIT_VAL;
static int hf_raptorcast_unix_ts_ms = INIT_VAL;
static int hf_raptorcast_delay = INIT_VAL;
static int hf_raptorcast_delay_ms = INIT_VAL;
static int hf_raptorcast_app_message_hash = INIT_VAL;
static int hf_raptorcast_app_message_len = INIT_VAL;
static int hf_raptorcast_merkle_root = INIT_VAL;
static int hf_raptorcast_merkle_proof[9] = { INIT_VAL };
static int hf_raptorcast_first_hop_recipient = INIT_VAL;
static int hf_raptorcast_merkle_leaf_index = INIT_VAL;
static int hf_raptorcast_reserved = INIT_VAL;
static int hf_raptorcast_encoding_symbol_id = INIT_VAL;
static int hf_raptorcast_encoded_symbol_len = INIT_VAL;
static int hf_raptorcast_payload_data = INIT_VAL;
static int hf_raptorcast_encoded_symbol = INIT_VAL;

static int ett_raptorcast = INIT_VAL;
static int ett_raptorcast_signature = INIT_VAL;
static int ett_raptorcast_merkle_proof = INIT_VAL;

#define header_offset(field)	(header_offset + offsetof(struct raptorcast_header, field))
#define header_size(field)	sizeof_member(struct raptorcast_header, field)

#define chunk_offset(field)	(chunk_header_offset + offsetof(struct raptorcast_chunk_header, field))
#define chunk_size(field)	sizeof_member(struct raptorcast_chunk_header, field)

enum signature_status {
	SIGNATURE_NONE,
	SIGNATURE_PARSE_ERROR,
	SIGNATURE_BAD,
	SIGNATURE_GOOD,
};

struct signature_cache_entry {
	uint8_t				author[33];
	struct raptorcast_signature	pkt_sig;
	uint8_t				msghash32[32];
};

static enum signature_status recover_author(uint8_t *author, size_t authorlen, const struct raptorcast_signature *pkt_sig, const uint8_t *msghash32)
{
	static secp256k1_context *ctx = NULL;
	static struct signature_cache_entry cache_entries[256];

	if (ctx == NULL) {
		ctx = secp256k1_context_create(SECP256K1_CONTEXT_VERIFY);
	}

	struct signature_cache_entry *entry = &cache_entries[msghash32[0]];

	if (!memcmp(pkt_sig, &entry->pkt_sig, sizeof(entry->pkt_sig)) && !memcmp(msghash32, entry->msghash32, sizeof(entry->msghash32))) {
		memcpy(author, entry->author, sizeof(entry->author));
		return SIGNATURE_GOOD;
	}

	secp256k1_ecdsa_recoverable_signature sig;

	// secp256k1_ecdsa_recoverable_signature_parse_compact() aborts if v >= 4
	if (pkt_sig->v < 0x04 && secp256k1_ecdsa_recoverable_signature_parse_compact(ctx, &sig, pkt_sig->r, pkt_sig->v) == 1) {
		secp256k1_pubkey pubkey;

		if (secp256k1_ecdsa_recover(ctx, &pubkey, &sig, msghash32) == 1) {
			secp256k1_ec_pubkey_serialize(ctx, author, &authorlen, &pubkey, SECP256K1_EC_COMPRESSED);

			memcpy(entry->author, author, sizeof(entry->author));
			memcpy(&entry->pkt_sig, pkt_sig, sizeof(entry->pkt_sig));
			memcpy(entry->msghash32, msghash32, sizeof(entry->msghash32));

			return SIGNATURE_GOOD;
		}

		return SIGNATURE_BAD;
	}

	return SIGNATURE_PARSE_ERROR;
}

static int
dissect_raptorcast(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree _U_, void *data _U_)
{
	const size_t header_offset = 0;
	const size_t header_size = sizeof(struct raptorcast_header);

	uint16_t version = 0;
	uint8_t merkle_tree_depth = 0;

	size_t merkle_proof_offset = 0;
	size_t merkle_proof_size = 0;
	size_t chunk_header_offset = 0;
	size_t chunk_header_size = 0;
	size_t payload_offset = 0;
	size_t payload_size = 0;
	uint8_t merkle_root[sizeof(struct raptorcast_merkle_item)];
	enum signature_status signature_status = SIGNATURE_NONE;
	uint8_t author[33];

	version = tvb_get_letohs(tvb, header_offset + offsetof(struct raptorcast_header, version));
	if (version == 0) {
		merkle_tree_depth = tvb_get_bits8(tvb, header_offset + offsetof(struct raptorcast_header, flags) * 8 + 1, 7);

		if (merkle_tree_depth >= 1 && merkle_tree_depth <= 9)  {
			merkle_proof_offset = header_offset + header_size;
			merkle_proof_size = (merkle_tree_depth - 1) * sizeof(struct raptorcast_merkle_item);

			chunk_header_offset = merkle_proof_offset + merkle_proof_size;
			chunk_header_size = sizeof(struct raptorcast_chunk_header);

			payload_offset = chunk_header_offset + chunk_header_size;
			payload_size = tvb_captured_length(tvb) - payload_offset;
		}
	}

	if (chunk_header_offset != 0) {
		blake3_hasher hasher;
		uint8_t hash[sizeof(struct raptorcast_merkle_item)];

		blake3_hasher_init(&hasher);
		size_t hash_data_length = sizeof(struct raptorcast_chunk_header) + payload_size;
		blake3_hasher_update(&hasher, tvb_get_ptr(tvb, chunk_header_offset, hash_data_length), hash_data_length);
		blake3_hasher_finalize(&hasher, hash, sizeof(hash));

		struct raptorcast_merkle_item *merkle_proof = (struct raptorcast_merkle_item *)tvb_get_ptr(tvb, merkle_proof_offset, merkle_proof_size);
		uint8_t merkle_leaf_index = tvb_get_uint8(tvb, chunk_offset(merkle_leaf_index));

		for (int i = 0; i < merkle_tree_depth - 1; i++) {
			blake3_hasher_init(&hasher);
			if ((merkle_leaf_index & (1 << i)) == 0) {
				blake3_hasher_update(&hasher, hash, sizeof(hash));
				blake3_hasher_update(&hasher, &merkle_proof[merkle_tree_depth - i - 2], sizeof(struct raptorcast_merkle_item));
			} else {
				blake3_hasher_update(&hasher, &merkle_proof[merkle_tree_depth - i - 2], sizeof(struct raptorcast_merkle_item));
				blake3_hasher_update(&hasher, hash, sizeof(hash));
			}
			blake3_hasher_finalize(&hasher, hash, sizeof(hash));
		}

		memcpy(merkle_root, hash, sizeof(merkle_root));
	}

	if (chunk_header_offset != 0) {
		blake3_hasher hasher;
		uint8_t msghash32[BLAKE3_OUT_LEN];

		blake3_hasher_init(&hasher);
		size_t signed_area_offset = header_offset + sizeof(struct raptorcast_signature);
		size_t signed_area_size = header_size - sizeof(struct raptorcast_signature);
		blake3_hasher_update(&hasher, tvb_get_ptr(tvb, signed_area_offset, signed_area_size), signed_area_size);
		blake3_hasher_update(&hasher, merkle_root, sizeof(merkle_root));
		blake3_hasher_finalize(&hasher, msghash32, sizeof(msghash32));

		const struct raptorcast_signature *pkt_sig = (const struct raptorcast_signature *)tvb_get_ptr(tvb, header_offset(signature), header_size(signature));

		signature_status = recover_author(author, sizeof(author), pkt_sig, msghash32);
	}


	col_set_str(pinfo->cinfo, COL_PROTOCOL, "RaptorCast");

	/* Clear out stuff in the info column */
	col_clear(pinfo->cinfo, COL_INFO);

	proto_item *ti = proto_tree_add_item(tree, proto_raptorcast, tvb, 0, -1, ENC_NA);

	proto_tree *raptorcast_tree = proto_item_add_subtree(ti, ett_raptorcast);

	if (signature_status == SIGNATURE_GOOD) {
		proto_item *author_item = proto_tree_add_bytes_with_length(raptorcast_tree, hf_raptorcast_author, tvb, 0, 0, author, sizeof(author));
		proto_item_set_generated(author_item);
	}

	const char *signature_text;
	switch (signature_status) {
	case SIGNATURE_NONE:
		signature_text = "Signature [internal error]";
		break;
	case SIGNATURE_PARSE_ERROR:
		signature_text = "Signature [parse error]";
		break;
	case SIGNATURE_BAD:
		signature_text = "Signature [validation error]";
		break;
	case SIGNATURE_GOOD:
		signature_text = "Signature";
		break;
	}

	proto_tree *signature_tree = proto_tree_add_subtree(raptorcast_tree, tvb, header_offset(signature), header_size(signature), ett_raptorcast_signature, NULL, signature_text);

	proto_tree_add_item(signature_tree, hf_raptorcast_signature_r, tvb, header_offset(signature.r), header_size(signature.r), ENC_NA);

	proto_tree_add_item(signature_tree, hf_raptorcast_signature_s, tvb, header_offset(signature.s), header_size(signature.s), ENC_NA);

	proto_item *signature_v_item = proto_tree_add_item(signature_tree, hf_raptorcast_signature_v, tvb, header_offset(signature.v), header_size(signature.v), ENC_NA);
	uint8_t signature_v = tvb_get_uint8(tvb, header_offset(signature.v));
	if (signature_v >= 0x04) {
		proto_item_append_text(signature_v_item, " [invalid]");
	}

	proto_item *signature_valid_item = proto_tree_add_boolean(signature_tree, hf_raptorcast_signature_valid, tvb, header_offset(signature), header_size(signature), signature_status == SIGNATURE_GOOD);
	proto_item_set_generated(signature_valid_item);

	proto_item *version_item = proto_tree_add_item(raptorcast_tree, hf_raptorcast_version, tvb, header_offset(version), header_size(version), ENC_LITTLE_ENDIAN);

	if (version == 0) {
		proto_tree_add_bits_item(raptorcast_tree, hf_raptorcast_flags_broadcast, tvb, header_offset(flags) * 8, 1, ENC_NA);
		proto_item *merkle_tree_depth_item = proto_tree_add_bits_item(raptorcast_tree, hf_raptorcast_flags_merkle_tree_depth, tvb, header_offset(flags) * 8 + 1, 7, ENC_NA);

		proto_tree_add_item(raptorcast_tree, hf_raptorcast_epoch, tvb, header_offset(epoch_no), header_size(epoch_no), ENC_LITTLE_ENDIAN);

		proto_item *unix_ts_ms_item = proto_tree_add_item(raptorcast_tree, hf_raptorcast_unix_ts_ms, tvb, header_offset(unix_ts_ms), header_size(unix_ts_ms), ENC_LITTLE_ENDIAN);

		uint64_t unix_ts_ms = tvb_get_letoh64(tvb, header_offset(unix_ts_ms));

		uint64_t packet_ts_ms = (1000 * ((uint64_t)pinfo->abs_ts.secs)) + (pinfo->abs_ts.nsecs / 1000000);
		int64_t delta_ms = packet_ts_ms - unix_ts_ms;

		if (unix_ts_ms < RAPTORCAST_MIN_TIMESTAMP) {
			proto_item_append_text(unix_ts_ms_item, " [invalid: %lu ms before RaptorCast protocol epoch]", (long)(RAPTORCAST_MIN_TIMESTAMP - unix_ts_ms));
		} else if (packet_ts_ms < unix_ts_ms) {
			proto_item_append_text(unix_ts_ms_item, " [invalid: %lu ms after packet reception]", (long)(unix_ts_ms - packet_ts_ms));
		} else {
			nstime_t delta = NSTIME_INIT_SECS_MSECS((delta_ms / 1000), (delta_ms % 1000));

			proto_item *delay_item = proto_tree_add_time(raptorcast_tree, hf_raptorcast_delay, tvb, header_offset(unix_ts_ms), header_size(unix_ts_ms), &delta);
			proto_item_set_generated(delay_item);
		}

		proto_item *delay_ms_item = proto_tree_add_int64(raptorcast_tree, hf_raptorcast_delay_ms, tvb, header_offset(unix_ts_ms), header_size(unix_ts_ms), delta_ms);
		proto_item_set_hidden(delay_ms_item);
		proto_item_set_generated(delay_ms_item);

		proto_tree_add_item(raptorcast_tree, hf_raptorcast_app_message_hash, tvb, header_offset(app_message_hash), header_size(app_message_hash), ENC_NA);

		proto_tree_add_item(raptorcast_tree, hf_raptorcast_app_message_len, tvb, header_offset(app_message_len), header_size(app_message_len), ENC_LITTLE_ENDIAN);
		uint32_t app_message_len = tvb_get_letohl(tvb, header_offset(app_message_len));

		if (merkle_tree_depth >= 1 && merkle_tree_depth <= 9)  {
			proto_tree *merkle_proof_tree = proto_tree_add_subtree(raptorcast_tree, tvb, merkle_proof_offset, merkle_proof_size, ett_raptorcast_merkle_proof, NULL, "Merkle proof");

			proto_item *merkle_root_item = proto_tree_add_bytes_with_length(merkle_proof_tree, hf_raptorcast_merkle_root, tvb, 0, 0, merkle_root, sizeof(merkle_root));
			proto_item_set_generated(merkle_root_item);
			for (int i = 0; i < merkle_tree_depth - 1; i++) {
				proto_tree_add_item(merkle_proof_tree, hf_raptorcast_merkle_proof[i], tvb, merkle_proof_offset + i * sizeof(struct raptorcast_merkle_item), sizeof(struct raptorcast_merkle_item), ENC_NA);
			}

			proto_tree_add_item(raptorcast_tree, hf_raptorcast_first_hop_recipient, tvb, chunk_offset(first_hop_recipient), chunk_size(first_hop_recipient), ENC_NA);

			proto_item *merkle_leaf_index_item = proto_tree_add_item(raptorcast_tree, hf_raptorcast_merkle_leaf_index, tvb, chunk_offset(merkle_leaf_index), chunk_size(merkle_leaf_index), ENC_NA);
			uint8_t merkle_leaf_index = tvb_get_uint8(tvb, chunk_offset(merkle_leaf_index));
			uint32_t max_merkle_leaf_index = 1 << (merkle_tree_depth - 1);
			if (merkle_leaf_index > max_merkle_leaf_index) {
				proto_item_append_text(merkle_leaf_index_item, " [invalid]");
			}

			proto_item *reserved_item = proto_tree_add_item(raptorcast_tree, hf_raptorcast_reserved, tvb, chunk_offset(reserved), chunk_size(reserved), ENC_NA);
			uint8_t reserved = tvb_get_uint8(tvb, chunk_offset(reserved));
			if (reserved != 0x00) {
				proto_item_append_text(reserved_item, " [invalid]");
			}

			proto_tree_add_item(raptorcast_tree, hf_raptorcast_encoding_symbol_id, tvb, chunk_offset(encoding_symbol_id), chunk_size(encoding_symbol_id), ENC_LITTLE_ENDIAN);
			uint16_t esi = tvb_get_letohs(tvb, chunk_offset(encoding_symbol_id));

			proto_item *encoded_symbol_len_item = proto_tree_add_uint(raptorcast_tree, hf_raptorcast_encoded_symbol_len, tvb, payload_offset, payload_size, payload_size);
			proto_item_set_hidden(encoded_symbol_len_item);

			if (app_message_len <= payload_size && esi < 7) {
				proto_tree_add_item(raptorcast_tree, hf_raptorcast_payload_data, tvb, payload_offset, app_message_len, ENC_NA);
			} else {
				proto_tree_add_item(raptorcast_tree, hf_raptorcast_encoded_symbol, tvb, payload_offset, payload_size, ENC_NA);
			}
		} else {
			proto_item_append_text(merkle_tree_depth_item, " [invalid]");
			col_add_fstr(pinfo->cinfo, COL_INFO,
				"Bogus Merkle tree depth (%u, must be 1 <= depth <= 9)", merkle_tree_depth);
		}
	} else {
		proto_item_append_text(version_item, " [invalid]");
		col_add_fstr(pinfo->cinfo, COL_INFO,
			"Bogus RaptorCast version (%u, must be 0)", version);
	}

	return tvb_captured_length(tvb);
}

void
proto_register_raptorcast(void)
{
	static hf_register_info hf[] = {
		{ &hf_raptorcast_author, {
			"Message author", "raptorcast.author", FT_BYTES, BASE_NONE,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_signature_r, {
			"r", "raptorcast.signature_r", FT_BYTES, BASE_NONE,
			NULL, 0, "Signature (r)", HFILL }},
		{ &hf_raptorcast_signature_s, {
			"s", "raptorcast.signature_s", FT_BYTES, BASE_NONE,
			NULL, 0, "Signature (s)", HFILL }},
		{ &hf_raptorcast_signature_v, {
			"v", "raptorcast.signature_v", FT_BYTES, BASE_NONE,
			NULL, 0, "Signature (v)", HFILL }},
		{ &hf_raptorcast_signature_valid, {
			"valid", "raptorcast.signature_valid", FT_BOOLEAN, BASE_NONE,
			NULL, 0, "Signature valid", HFILL }},
		{ &hf_raptorcast_version, {
			"Version", "raptorcast.version", FT_UINT16, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_flags_broadcast, {
			"Broadcast", "raptorcast.broadcast", FT_BOOLEAN, BASE_NONE,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_flags_merkle_tree_depth, {
			"Merkle tree depth", "raptorcast.merkle_tree_depth", FT_UINT8, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_epoch, {
			"Epoch", "raptorcast.epoch", FT_UINT64, BASE_DEC,
			NULL, 0, "Consensus epoch this message was originated in", HFILL }},
		{ &hf_raptorcast_unix_ts_ms, {
			"Message timestamp", "raptorcast.unix_ts_ms", FT_UINT64, BASE_DEC,
			NULL, 0, "Message timestamp (milliseconds since Unix epoch)", HFILL }},
		{ &hf_raptorcast_delay, {
			"Packet delay", "raptorcast.delay", FT_RELATIVE_TIME, BASE_NONE,
			NULL, 0, "Packet delay (time between message construction and packet capture)", HFILL }},
		{ &hf_raptorcast_delay_ms, {
			"Packet delay (ms)", "raptorcast.delay_ms", FT_INT64, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_app_message_hash, {
			"Application message hash", "raptorcast.app_message_hash", FT_BYTES, BASE_NONE,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_app_message_len, {
			"Application message length", "raptorcast.app_message_len", FT_UINT32, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_merkle_root, {
			"Root", "raptorcast.merkle_root", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle root", HFILL }},
		{ &hf_raptorcast_merkle_proof[0], {
			"Level 1", "raptorcast.merkle_proof_1", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 1)", HFILL }},
		{ &hf_raptorcast_merkle_proof[1], {
			"Level 2", "raptorcast.merkle_proof_2", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 2)", HFILL }},
		{ &hf_raptorcast_merkle_proof[2], {
			"Level 3", "raptorcast.merkle_proof_3", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 3)", HFILL }},
		{ &hf_raptorcast_merkle_proof[3], {
			"Level 4", "raptorcast.merkle_proof_4", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 4)", HFILL }},
		{ &hf_raptorcast_merkle_proof[4], {
			"Level 5", "raptorcast.merkle_proof_5", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 5)", HFILL }},
		{ &hf_raptorcast_merkle_proof[5], {
			"Level 6", "raptorcast.merkle_proof_6", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 6)", HFILL }},
		{ &hf_raptorcast_merkle_proof[6], {
			"Level 7", "raptorcast.merkle_proof_7", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 7)", HFILL }},
		{ &hf_raptorcast_merkle_proof[7], {
			"Level 8", "raptorcast.merkle_proof_8", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 8)", HFILL }},
		{ &hf_raptorcast_merkle_proof[8], {
			"Level 9", "raptorcast.merkle_proof_9", FT_BYTES, BASE_NONE,
			NULL, 0, "Merkle proof (level 9)", HFILL }},
		{ &hf_raptorcast_first_hop_recipient, {
			"First-hop recipient", "raptorcast.first_hop_recipient", FT_BYTES, BASE_NONE,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_merkle_leaf_index, {
			"Merkle leaf index", "raptorcast.merkle_leaf_index", FT_UINT8, BASE_HEX,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_reserved, {
			"Reserved", "raptorcast.reserved", FT_UINT8, BASE_HEX,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_encoding_symbol_id, {
			"Encoding symbol ID", "raptorcast.encoding_symbol_id", FT_UINT16, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_encoded_symbol_len, {
			"Encoded symbol length", "raptorcast.encoded_symbol_len", FT_UINT32, BASE_DEC,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_payload_data, {
			"Payload data", "raptorcast.payload_data", FT_BYTES, BASE_NONE,
			NULL, 0, NULL, HFILL }},
		{ &hf_raptorcast_encoded_symbol, {
			"Encoded symbol", "raptorcast.encoded_symbol", FT_BYTES, BASE_NONE,
			NULL, 0, NULL, HFILL }},
	};

	static int *ett[] = {
		&ett_raptorcast,
		&ett_raptorcast_signature,
		&ett_raptorcast_merkle_proof
	};

	proto_raptorcast = proto_register_protocol (
		"RaptorCast Protocol",	/* name        */
		"RaptorCast",		/* short_name  */
		"raptorcast"		/* filter_name */
		);

	proto_register_field_array(proto_raptorcast, hf, array_length(hf));
	proto_register_subtree_array(ett, array_length(ett));
}

void
proto_reg_handoff_raptorcast(void)
{
	static dissector_handle_t raptorcast_handle;

	raptorcast_handle = create_dissector_handle(dissect_raptorcast, proto_raptorcast);

	dissector_add_uint_with_preference("udp.port", RAPTORCAST_PORT, raptorcast_handle);
}

void
plugin_register(void)
{
	static proto_plugin plug;

	plug.register_protoinfo = proto_register_raptorcast;
	plug.register_handoff = proto_reg_handoff_raptorcast;
	proto_register_plugin(&plug);
}

#if (WIRESHARK_VERSION_MAJOR == 4 && WIRESHARK_VERSION_MINOR >= 3) || WIRESHARK_VERSION_MAJOR > 4
uint32_t
plugin_describe(void)
{
	return WS_PLUGIN_DESC_DISSECTOR;
}
#endif
