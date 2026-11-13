// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js

package webrtc

import (
	"math"
	"strconv"

	"github.com/pion/interceptor/pkg/stats"
)

// Stats IDs for the sender-side stats objects. The RTPSender id itself is
// generated randomly, these deterministic suffixes keep the related objects
// addressable and make the ID graph stable across GetStats calls.
const (
	rtpSenderStatsPrefix         = "rtp-sender-"
	senderTrackStatsSuffix       = "-track"
	senderMediaSourceStatsSuffix = "-media-source"
	outboundRTPStatsPrefix       = "outbound-rtp-"
)

func (r *RTPSender) senderStatsID() string {
	return rtpSenderStatsPrefix + r.id
}

func (r *RTPSender) trackStatsID() string {
	return r.senderStatsID() + senderTrackStatsSuffix
}

func (r *RTPSender) mediaSourceStatsID() string {
	return r.senderStatsID() + senderMediaSourceStatsSuffix
}

func outboundRTPStatsID(ssrc SSRC) string {
	return outboundRTPStatsPrefix + strconv.FormatUint(uint64(ssrc), 10)
}

// collectStats emits the sender-side stats objects: one OutboundRTPStreamStats
// per encoding SSRC, plus the sender, track attachment and media-source objects
// referenced by them.
//
// It works without the stats interceptor installed: interceptor-backed counters
// stay at their zero values in that case and the call never blocks or panics.
func (r *RTPSender) collectStats(collector *statsReportCollector, statsGetter stats.Getter) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := statsTimestampNow()
	mid := ""
	if r.rtpTransceiver != nil {
		mid = r.rtpTransceiver.Mid()
	}

	senderID := r.senderStatsID()
	trackID := r.trackStatsID()
	mediaSourceID := r.mediaSourceStatsID()
	kind := r.kind.String()

	// Emit one outbound-rtp entry per encoding SSRC. RTX and FEC SSRCs are not
	// reported as separate streams: their traffic is accounted against the
	// primary SSRC by the stats interceptor.
	for _, encoding := range r.trackEncodings {
		if encoding.ssrc == 0 {
			continue
		}

		collector.Collecting()

		rid := encoding.rid
		active := encoding.track != nil
		outboundID := outboundRTPStatsID(encoding.ssrc)
		outboundStats := OutboundRTPStreamStats{
			Mid:           mid,
			Rid:           rid,
			Timestamp:     now,
			Type:          StatsTypeOutboundRTP,
			ID:            outboundID,
			SSRC:          encoding.ssrc,
			Kind:          kind,
			TransportID:   "iceTransport",
			MediaSourceID: mediaSourceID,
			TrackID:       trackID,
			SenderID:      senderID,
			Active:        active,
		}
		r.populateOutboundStats(&outboundStats, statsGetter, encoding.ssrc)

		collector.Collect(outboundID, outboundStats)
	}

	// The sender stats object appears as soon as the RTPSender exists, even when
	// no track is currently attached.
	collector.Collecting()
	if r.kind == RTPCodecTypeAudio {
		collector.Collect(senderID, AudioSenderStats{
			Timestamp: now,
			Type:      StatsTypeSender,
			ID:        senderID,
			Kind:      kind,
		})
	} else {
		collector.Collect(senderID, VideoSenderStats{
			Timestamp: now,
			Type:      StatsTypeSender,
			ID:        senderID,
			Kind:      kind,
		})
	}

	// The track attachment stats object stays present after ReplaceTrack(nil)
	// so that the ID graph of existing outbound-rtp entries stays resolvable.
	var track TrackLocal
	if len(r.trackEncodings) != 0 {
		track = r.trackEncodings[0].track
	}
	trackIdentifier := ""
	ended := track == nil
	if track != nil {
		trackIdentifier = track.ID()
	}
	collector.Collecting()
	if r.kind == RTPCodecTypeAudio {
		collector.Collect(trackID, SenderAudioTrackAttachmentStats{
			Timestamp:       now,
			Type:            StatsTypeTrack,
			ID:              trackID,
			TrackIdentifier: trackIdentifier,
			Ended:           ended,
			Kind:            kind,
		})
	} else {
		collector.Collect(trackID, SenderVideoTrackAttachmentStats{
			Timestamp: now,
			Type:      StatsTypeTrack,
			ID:        trackID,
			Kind:      kind,
		})
	}

	// The media source describes the track feeding the sender. It is kept around
	// while the sender exists so that MediaSourceID references never dangle.
	collector.Collecting()
	if r.kind == RTPCodecTypeAudio {
		collector.Collect(mediaSourceID, AudioSourceStats{
			Timestamp:       now,
			Type:            StatsTypeMediaSource,
			ID:              mediaSourceID,
			TrackIdentifier: trackIdentifier,
			Kind:            kind,
		})
	} else {
		collector.Collect(mediaSourceID, VideoSourceStats{
			Timestamp:       now,
			Type:            StatsTypeMediaSource,
			ID:              mediaSourceID,
			TrackIdentifier: trackIdentifier,
			Kind:            kind,
		})
	}
}

func (r *RTPSender) populateOutboundStats(
	outboundStats *OutboundRTPStreamStats,
	statsGetter stats.Getter,
	ssrc SSRC,
) {
	if statsGetter == nil {
		return
	}

	streamStats := statsGetter.Get(uint32(ssrc))
	if streamStats == nil {
		return
	}

	// Wrap-around casting by design, with a warning when overflow is detected.
	ps := streamStats.OutboundRTPStreamStats.PacketsSent
	if ps > math.MaxUint32 {
		r.log.Warnf("Outbound PacketsSent exceeds uint32 and will wrap: %d", ps)
	}
	outboundStats.PacketsSent = uint32(ps) //nolint:gosec // G115

	outboundStats.BytesSent = streamStats.OutboundRTPStreamStats.BytesSent
	outboundStats.HeaderBytesSent = streamStats.OutboundRTPStreamStats.HeaderBytesSent
	outboundStats.NACKCount = streamStats.OutboundRTPStreamStats.NACKCount
	outboundStats.FIRCount = streamStats.OutboundRTPStreamStats.FIRCount
	outboundStats.PLICount = streamStats.OutboundRTPStreamStats.PLICount
}
