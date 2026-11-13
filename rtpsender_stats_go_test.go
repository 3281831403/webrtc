// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js

package webrtc

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func findOutboundRTPStats(report StatsReport) []OutboundRTPStreamStats {
	result := []OutboundRTPStreamStats{}
	for _, s := range report {
		if stats, ok := s.(OutboundRTPStreamStats); ok {
			result = append(result, stats)
		}
	}

	return result
}

func findOutboundRTPStatsBySSRC(report StatsReport, ssrc SSRC) []OutboundRTPStreamStats {
	result := []OutboundRTPStreamStats{}
	for _, s := range report {
		if stats, ok := s.(OutboundRTPStreamStats); ok && stats.SSRC == ssrc {
			result = append(result, stats)
		}
	}

	return result
}

// requireReferencedStatsExist verifies every ID referenced by an outbound-rtp
// entry is itself present in the report.
func requireReferencedStatsExist(t *testing.T, report StatsReport, outbound OutboundRTPStreamStats) {
	t.Helper()

	for _, id := range []string{outbound.SenderID, outbound.TrackID, outbound.MediaSourceID, outbound.TransportID} {
		require.NotEmpty(t, id, "referenced stats ID must not be empty")
		_, ok := report[id]
		require.True(t, ok, "referenced stats ID %q missing from report", id)
	}
}

func writeTestRTPPackets(t *testing.T, track *TrackLocalStaticRTP, count int, payloadSize int) {
	t.Helper()

	for i := 0; i < count; i++ {
		packet := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    96,
				SequenceNumber: uint16(i), //nolint:gosec // G115
				Timestamp:      uint32(i * 1000),
				SSRC:           0, // set by the track binding
			},
			Payload: make([]byte, payloadSize),
		}
		require.NoError(t, track.WriteRTP(packet))
	}
}

func TestPeerConnection_GetStats_OutboundRTP(t *testing.T) {
	offerPC, answerPC, err := newPair()
	require.NoError(t, err)
	defer closePairNow(t, offerPC, answerPC)

	track, err := NewTrackLocalStaticRTP(
		RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion",
	)
	require.NoError(t, err)

	sender, err := offerPC.AddTrack(track)
	require.NoError(t, err)

	// Sender-side objects exist as soon as the track is added, before negotiation.
	preNegotiationReport := offerPC.GetStats()
	require.Len(t, findOutboundRTPStats(preNegotiationReport), 1)
	preOutbound := findOutboundRTPStats(preNegotiationReport)[0]
	requireReferencedStatsExist(t, preNegotiationReport, preOutbound)
	assert.Equal(t, StatsTypeOutboundRTP, preOutbound.Type)
	assert.Equal(t, "video", preOutbound.Kind)
	assert.True(t, preOutbound.Active)

	packetReceived := make(chan struct{}, 1)
	answerPC.OnTrack(func(remote *TrackRemote, _ *RTPReceiver) {
		go func() {
			if _, _, readErr := remote.ReadRTP(); readErr == nil {
				packetReceived <- struct{}{}
			}
		}()
	})

	require.NoError(t, signalPairForStats(offerPC, answerPC))
	waitConnectedForStats(t, offerPC, answerPC)

	const packetCount = 10
	const payloadSize = 100
	writeTestRTPPackets(t, track, packetCount, payloadSize)

	select {
	case <-packetReceived:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for the first RTP packet")
	}

	parameters := sender.GetParameters()
	require.NotEmpty(t, parameters.Encodings)
	ssrc := parameters.Encodings[0].SSRC
	require.NotZero(t, ssrc)

	var outbound OutboundRTPStreamStats
	require.Eventually(t, func() bool {
		matches := findOutboundRTPStatsBySSRC(offerPC.GetStats(), ssrc)
		if len(matches) == 0 {
			return false
		}
		outbound = matches[0]

		return outbound.PacketsSent >= packetCount
	}, 2*time.Second, 10*time.Millisecond, "outbound packetsSent never reached the expected value")

	report := offerPC.GetStats()
	matches := findOutboundRTPStatsBySSRC(report, ssrc)
	require.Len(t, matches, 1)
	outbound = matches[0]

	assert.Equal(t, StatsTypeOutboundRTP, outbound.Type)
	assert.Equal(t, ssrc, outbound.SSRC)
	assert.Equal(t, "video", outbound.Kind)
	assert.True(t, outbound.Active)
	assert.NotEmpty(t, outbound.Mid)
	assert.Empty(t, outbound.Rid, "non-simulcast video must not carry a rid")
	assert.Equal(t, uint32(packetCount), outbound.PacketsSent)
	assert.Equal(t, uint64(packetCount*payloadSize), outbound.BytesSent,
		"bytesSent must count payload bytes only")
	assert.Greater(t, outbound.HeaderBytesSent, uint64(0),
		"headerBytesSent must be tracked separately from payload bytes")
	requireReferencedStatsExist(t, report, outbound)

	// The referenced sender/track/media-source objects have the expected types.
	_, ok := report[outbound.SenderID].(VideoSenderStats)
	require.True(t, ok, "senderId must resolve to a VideoSenderStats entry")
	_, ok = report[outbound.TrackID].(SenderVideoTrackAttachmentStats)
	require.True(t, ok, "trackId must resolve to a track attachment entry")
	_, ok = report[outbound.MediaSourceID].(VideoSourceStats)
	require.True(t, ok, "mediaSourceId must resolve to a media-source entry")
	transportStats, ok := report[outbound.TransportID].(TransportStats)
	require.True(t, ok, "transportId must resolve to a TransportStats entry")
	assert.Equal(t, "iceTransport", transportStats.ID)
}

func TestPeerConnection_GetStats_OutboundRTP_ReplaceTrackPauses(t *testing.T) {
	offerPC, answerPC, err := newPair()
	require.NoError(t, err)
	defer closePairNow(t, offerPC, answerPC)

	track, err := NewTrackLocalStaticRTP(
		RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion",
	)
	require.NoError(t, err)

	sender, err := offerPC.AddTrack(track)
	require.NoError(t, err)

	packetReceived := make(chan struct{}, 16)
	answerPC.OnTrack(func(remote *TrackRemote, _ *RTPReceiver) {
		go func() {
			for {
				if _, _, readErr := remote.ReadRTP(); readErr != nil {
					return
				}
				select {
				case packetReceived <- struct{}{}:
				default:
				}
			}
		}()
	})

	require.NoError(t, signalPairForStats(offerPC, answerPC))
	waitConnectedForStats(t, offerPC, answerPC)

	const firstBatch = 5
	writeTestRTPPackets(t, track, firstBatch, 50)
	select {
	case <-packetReceived:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for the first RTP packet")
	}

	ssrc := sender.GetParameters().Encodings[0].SSRC

	var frozen OutboundRTPStreamStats
	require.Eventually(t, func() bool {
		matches := findOutboundRTPStatsBySSRC(offerPC.GetStats(), ssrc)
		if len(matches) == 0 || matches[0].PacketsSent < firstBatch {
			return false
		}
		frozen = matches[0]

		return true
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, sender.ReplaceTrack(nil))

	// The entry survives the pause, is marked inactive, and keeps its counters.
	pausedReport := offerPC.GetStats()
	pausedMatches := findOutboundRTPStatsBySSRC(pausedReport, ssrc)
	require.Len(t, pausedMatches, 1)
	paused := pausedMatches[0]
	assert.False(t, paused.Active, "paused outbound stream must report active=false")
	assert.Equal(t, frozen.PacketsSent, paused.PacketsSent)
	assert.Equal(t, frozen.BytesSent, paused.BytesSent)
	assert.Equal(t, frozen.HeaderBytesSent, paused.HeaderBytesSent)
	requireReferencedStatsExist(t, pausedReport, paused)

	// Nothing flows while paused: counters remain frozen across repeated calls.
	time.Sleep(50 * time.Millisecond)
	pausedReport = offerPC.GetStats()
	paused = findOutboundRTPStatsBySSRC(pausedReport, ssrc)[0]
	assert.Equal(t, frozen.PacketsSent, paused.PacketsSent)
	assert.Equal(t, frozen.BytesSent, paused.BytesSent)

	// Re-attach a track: the same SSRC entry resumes and counters continue upward.
	replacement, err := NewTrackLocalStaticRTP(
		RTPCodecCapability{MimeType: MimeTypeVP8}, "video-replacement", "pion",
	)
	require.NoError(t, err)
	require.NoError(t, sender.ReplaceTrack(replacement))

	const secondBatch = 7
	writeTestRTPPackets(t, replacement, secondBatch, 60)

	var resumed OutboundRTPStreamStats
	require.Eventually(t, func() bool {
		matches := findOutboundRTPStatsBySSRC(offerPC.GetStats(), ssrc)
		if len(matches) == 0 {
			return false
		}
		resumed = matches[0]

		return resumed.Active && resumed.PacketsSent >= frozen.PacketsSent+secondBatch
	}, 2*time.Second, 10*time.Millisecond, "outbound counters did not resume after ReplaceTrack")

	assert.Equal(t, frozen.PacketsSent+secondBatch, resumed.PacketsSent)
}

func TestPeerConnection_GetStats_OutboundRTP_Simulcast(t *testing.T) {
	offerPC, answerPC, err := newPair()
	require.NoError(t, err)
	defer closePairNow(t, offerPC, answerPC)

	rids := []string{"low", "mid", "high"}
	tracks := make([]*TrackLocalStaticRTP, 3)
	for i, rid := range rids {
		writer, writerErr := NewTrackLocalStaticRTP(
			RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID(rid),
		)
		require.NoError(t, writerErr)
		tracks[i] = writer
	}

	sender, err := offerPC.AddTrack(tracks[0])
	require.NoError(t, err)
	require.NoError(t, sender.AddEncoding(tracks[1]))
	require.NoError(t, sender.AddEncoding(tracks[2]))

	require.NoError(t, signalPairForStats(offerPC, answerPC))
	waitConnectedForStats(t, offerPC, answerPC)

	encodings := sender.GetParameters().Encodings
	require.Len(t, encodings, 3)

	const packetsPerLayer = 4
	for _, writer := range tracks {
		writeTestRTPPackets(t, writer, packetsPerLayer, 40)
	}

	report := offerPC.GetStats()
	outbound := findOutboundRTPStats(report)
	require.Len(t, outbound, 3, "simulcast must produce one outbound-rtp entry per encoding SSRC")

	ssrcToRid := map[SSRC]string{}
	for i, encoding := range encodings {
		ssrcToRid[encoding.SSRC] = rids[i]
		require.NotZero(t, encoding.SSRC)
	}

	seenRids := map[string]bool{}
	for _, entry := range outbound {
		expectedRid, ok := ssrcToRid[entry.SSRC]
		require.True(t, ok, "unexpected SSRC in outbound stats: %d", entry.SSRC)
		assert.Equal(t, expectedRid, entry.Rid)
		assert.NotEmpty(t, entry.Mid)
		assert.True(t, entry.Active)
		assert.Equal(t, uint32(packetsPerLayer), entry.PacketsSent)
		assert.Equal(t, uint64(packetsPerLayer*40), entry.BytesSent)
		assert.Greater(t, entry.HeaderBytesSent, uint64(0))
		requireReferencedStatsExist(t, report, entry)
		seenRids[entry.Rid] = true
	}
	assert.ElementsMatch(t, rids, keysOfRidSet(seenRids))
}

func keysOfRidSet(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}

	return keys
}

func waitConnectedForStats(t *testing.T, pcs ...*PeerConnection) {
	t.Helper()

	select {
	case <-untilConnectionState(PeerConnectionStateConnected, pcs...):
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for PeerConnection connected state")
	}
}

// Without the stats interceptor, GetStats must still emit the sender-side
// objects with zeroed counters instead of hanging or panicking.
func TestPeerConnection_GetStats_OutboundRTP_NoStatsInterceptor(t *testing.T) {
	registry := &interceptor.Registry{}
	api := NewAPI(WithInterceptorRegistry(registry))

	offerPC, err := api.NewPeerConnection(Configuration{})
	require.NoError(t, err)
	answerPC, err := NewPeerConnection(Configuration{})
	require.NoError(t, err)
	defer closePairNow(t, offerPC, answerPC)

	track, err := NewTrackLocalStaticRTP(
		RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion",
	)
	require.NoError(t, err)

	sender, err := offerPC.AddTrack(track)
	require.NoError(t, err)

	done := make(chan StatsReport, 1)
	go func() {
		done <- offerPC.GetStats()
	}()
	select {
	case report := <-done:
		outbound := findOutboundRTPStats(report)
		require.Len(t, outbound, 1)
		assert.Zero(t, outbound[0].PacketsSent)
		assert.Zero(t, outbound[0].BytesSent)
		assert.Zero(t, outbound[0].HeaderBytesSent)
		assert.True(t, outbound[0].Active)
		requireReferencedStatsExist(t, report, outbound[0])
	case <-time.After(3 * time.Second):
		require.FailNow(t, "GetStats hung without a stats interceptor")
	}

	// Negotiation and actual sending must also stay functional.
	packetReceived := make(chan struct{}, 1)
	answerPC.OnTrack(func(remote *TrackRemote, _ *RTPReceiver) {
		go func() {
			if _, _, readErr := remote.ReadRTP(); readErr == nil {
				packetReceived <- struct{}{}
			}
		}()
	})
	require.NoError(t, signalPairForStats(offerPC, answerPC))
	waitConnectedForStats(t, offerPC, answerPC)
	writeTestRTPPackets(t, track, 3, 30)
	select {
	case <-packetReceived:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for RTP packet without stats interceptor")
	}

	ssrc := sender.GetParameters().Encodings[0].SSRC
	report := offerPC.GetStats()
	matches := findOutboundRTPStatsBySSRC(report, ssrc)
	require.Len(t, matches, 1)
	// Counters stay zero because no interceptor is recording, but the entry exists.
	assert.Zero(t, matches[0].PacketsSent)
	assert.True(t, matches[0].Active)
	requireReferencedStatsExist(t, report, matches[0])
}

func TestPeerConnection_GetStats_OutboundRTP_Audio(t *testing.T) {
	offerPC, answerPC, err := newPair()
	require.NoError(t, err)
	defer closePairNow(t, offerPC, answerPC)

	track, err := NewTrackLocalStaticSample(
		RTPCodecCapability{MimeType: MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "pion",
	)
	require.NoError(t, err)

	sender, err := offerPC.AddTrack(track)
	require.NoError(t, err)

	packetReceived := make(chan struct{}, 1)
	answerPC.OnTrack(func(remote *TrackRemote, _ *RTPReceiver) {
		go func() {
			if _, _, readErr := remote.ReadRTP(); readErr == nil {
				packetReceived <- struct{}{}
			}
		}()
	})

	require.NoError(t, signalPairForStats(offerPC, answerPC))
	waitConnectedForStats(t, offerPC, answerPC)
	require.NoError(t, track.WriteSample(media.Sample{
		Data:     make([]byte, 160),
		Duration: 20 * time.Millisecond,
	}))

	select {
	case <-packetReceived:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for the first audio RTP packet")
	}

	ssrc := sender.GetParameters().Encodings[0].SSRC
	var outbound OutboundRTPStreamStats
	require.Eventually(t, func() bool {
		matches := findOutboundRTPStatsBySSRC(offerPC.GetStats(), ssrc)
		if len(matches) == 0 || matches[0].PacketsSent == 0 {
			return false
		}
		outbound = matches[0]

		return true
	}, 2*time.Second, 10*time.Millisecond)

	report := offerPC.GetStats()
	outbound = findOutboundRTPStatsBySSRC(report, ssrc)[0]
	assert.Equal(t, "audio", outbound.Kind)
	assert.Empty(t, outbound.Rid, "audio must never carry a rid")
	assert.Greater(t, outbound.PacketsSent, uint32(0))
	requireReferencedStatsExist(t, report, outbound)

	_, ok := report[outbound.SenderID].(AudioSenderStats)
	require.True(t, ok, "senderId must resolve to an AudioSenderStats entry")
	_, ok = report[outbound.TrackID].(SenderAudioTrackAttachmentStats)
	require.True(t, ok, "trackId must resolve to an audio track attachment entry")
	_, ok = report[outbound.MediaSourceID].(AudioSourceStats)
	require.True(t, ok, "mediaSourceId must resolve to an audio media-source entry")
}
