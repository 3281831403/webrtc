// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js

package webrtc

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/transport/v5/test"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/assert"
)

func Test_RTPSender_ReplaceTrack(t *testing.T) { //nolint:cyclop
	lim := test.TimeOut(time.Second * 10)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	s := SettingEngine{}
	s.DisableSRTPReplayProtection(true)

	sender, receiver, err := NewAPI(WithSettingEngine(s)).newPair(Configuration{})
	assert.NoError(t, err)

	trackA, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	trackB, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeH264}, "video", "pion")
	assert.NoError(t, err)

	rtpSender, err := sender.AddTrack(trackA)
	assert.NoError(t, err)

	seenPacketA, seenPacketACancel := context.WithCancel(context.Background())
	seenPacketB, seenPacketBCancel := context.WithCancel(context.Background())

	var onTrackCount uint64
	receiver.OnTrack(func(track *TrackRemote, _ *RTPReceiver) {
		assert.Equal(t, uint64(1), atomic.AddUint64(&onTrackCount, 1))

		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				assert.True(t, errors.Is(err, io.EOF))

				return
			}

			switch {
			case pkt.Payload[len(pkt.Payload)-1] == 0xAA:
				assert.Equal(t, track.Codec().MimeType, MimeTypeVP8)
				seenPacketACancel()
			case pkt.Payload[len(pkt.Payload)-1] == 0xBB:
				assert.Equal(t, track.Codec().MimeType, MimeTypeH264)
				seenPacketBCancel()
			default:
				assert.Failf(t, "Unexpected RTP", "Data % 02x", pkt.Payload[len(pkt.Payload)-1])
			}
		}
	})

	assert.NoError(t, signalPair(sender, receiver))

	// Block Until packet with 0xAA has been seen
	func() {
		for range time.Tick(time.Millisecond * 20) {
			select {
			case <-seenPacketA.Done():
				return
			default:
				assert.NoError(t, trackA.WriteSample(media.Sample{Data: []byte{0xAA}, Duration: time.Second}))
			}
		}
	}()

	assert.NoError(t, rtpSender.ReplaceTrack(trackB))

	// Block Until packet with 0xBB has been seen
	func() {
		for range time.Tick(time.Millisecond * 20) {
			select {
			case <-seenPacketB.Done():
				return
			default:
				assert.NoError(t, trackB.WriteSample(media.Sample{Data: []byte{0xBB}, Duration: time.Second}))
			}
		}
	}()

	closePairNow(t, sender, receiver)
}

func Test_RTPSender_GetParameters(t *testing.T) {
	lim := test.TimeOut(time.Second * 10)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	offerer, answerer, err := newPair()
	assert.NoError(t, err)

	rtpTransceiver, err := offerer.AddTransceiverFromKind(RTPCodecTypeVideo)
	assert.NoError(t, err)

	assert.NoError(t, signalPair(offerer, answerer))

	parameters := rtpTransceiver.Sender().GetParameters()
	assert.NotEqual(t, 0, len(parameters.Codecs))
	assert.Equal(t, 1, len(parameters.Encodings))
	assert.Equal(t, rtpTransceiver.Sender().trackEncodings[0].ssrc, parameters.Encodings[0].SSRC)
	assert.Equal(t, "", parameters.Encodings[0].RID)

	closePairNow(t, offerer, answerer)
}

func Test_RTPSender_GetParameters_WithRID(t *testing.T) {
	lim := test.TimeOut(time.Second * 10)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	offerer, answerer, err := newPair()
	assert.NoError(t, err)

	rtpTransceiver, err := offerer.AddTransceiverFromKind(RTPCodecTypeVideo)
	assert.NoError(t, err)

	assert.NoError(t, signalPair(offerer, answerer))

	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("moo"))
	assert.NoError(t, err)

	err = rtpTransceiver.setSendingTrack(track)
	assert.NoError(t, err)

	parameters := rtpTransceiver.Sender().GetParameters()
	assert.Equal(t, track.RID(), parameters.Encodings[0].RID)

	closePairNow(t, offerer, answerer)
}

func Test_RTPSender_SetReadDeadline(t *testing.T) {
	lim := test.TimeOut(time.Second * 30)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	sender, receiver, wan := createVNetPair(t, &interceptor.Registry{})

	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	rtpSender, err := sender.AddTrack(track)
	assert.NoError(t, err)

	peerConnectionsConnected := untilConnectionState(PeerConnectionStateConnected, sender, receiver)

	assert.NoError(t, signalPair(sender, receiver))

	<-peerConnectionsConnected

	assert.NoError(t, rtpSender.SetReadDeadline(time.Now().Add(1*time.Second)))
	_, _, err = rtpSender.ReadRTCP()
	assert.Error(t, err)

	assert.NoError(t, wan.Stop())
	closePairNow(t, sender, receiver)
}

func Test_RTPSender_ReplaceTrack_InvalidTrackKindChange(t *testing.T) {
	lim := test.TimeOut(time.Second * 10)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	sender, receiver, err := newPair()
	assert.NoError(t, err)

	trackA, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	trackB, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeOpus}, "audio", "pion")
	assert.NoError(t, err)

	rtpSender, err := sender.AddTrack(trackA)
	assert.NoError(t, err)

	assert.NoError(t, signalPair(sender, receiver))

	seenPacket, seenPacketCancel := context.WithCancel(context.Background())
	receiver.OnTrack(func(_ *TrackRemote, _ *RTPReceiver) {
		seenPacketCancel()
	})

	func() {
		for range time.Tick(time.Millisecond * 20) {
			select {
			case <-seenPacket.Done():
				return
			default:
				assert.NoError(t, trackA.WriteSample(media.Sample{Data: []byte{0xAA}, Duration: time.Second}))
			}
		}
	}()

	assert.True(t, errors.Is(rtpSender.ReplaceTrack(trackB), ErrRTPSenderNewTrackHasIncorrectKind))

	closePairNow(t, sender, receiver)
}

func Test_RTPSender_ReplaceTrack_InvalidCodecChange(t *testing.T) {
	lim := test.TimeOut(time.Second * 10)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	sender, receiver, err := newPair()
	assert.NoError(t, err)

	trackA, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	trackB, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP9}, "video", "pion")
	assert.NoError(t, err)

	rtpSender, err := sender.AddTrack(trackA)
	assert.NoError(t, err)

	err = rtpSender.rtpTransceiver.SetCodecPreferences([]RTPCodecParameters{{RTPCodecCapability: RTPCodecCapability{MimeType: MimeTypeVP8}, PayloadType: 96}})
	assert.NoError(t, err)

	assert.NoError(t, signalPair(sender, receiver))

	seenPacket, seenPacketCancel := context.WithCancel(context.Background())
	receiver.OnTrack(func(_ *TrackRemote, _ *RTPReceiver) {
		seenPacketCancel()
	})

	func() {
		for range time.Tick(time.Millisecond * 20) {
			select {
			case <-seenPacket.Done():
				return
			default:
				assert.NoError(t, trackA.WriteSample(media.Sample{Data: []byte{0xAA}, Duration: time.Second}))
			}
		}
	}()

	assert.True(t, errors.Is(rtpSender.ReplaceTrack(trackB), ErrUnsupportedCodec))

	closePairNow(t, sender, receiver)
}

func Test_RTPSender_GetParameters_NilTrack(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	rtpSender, err := peerConnection.AddTrack(track)
	assert.NoError(t, err)

	assert.NoError(t, rtpSender.ReplaceTrack(nil))
	rtpSender.GetParameters()

	assert.NoError(t, peerConnection.Close())
}

// errBindTrack is a TrackLocal whose Bind always fails, used to verify that a
// failed ReplaceTrack rolls back and leaves the original stream untouched.
type errBindTrack struct{}

var errBindTrackBind = errors.New("errBindTrack: bind failed")

func (e *errBindTrack) Bind(TrackLocalContext) (RTPCodecParameters, error) {
	return RTPCodecParameters{}, errBindTrackBind
}

func (e *errBindTrack) Unbind(TrackLocalContext) error { return nil }
func (e *errBindTrack) ID() string                     { return "errBindTrack" }
func (e *errBindTrack) RID() string                    { return "" }
func (e *errBindTrack) StreamID() string               { return "pion" }
func (e *errBindTrack) Kind() RTPCodecType             { return RTPCodecTypeVideo }

// Test_RTPSender_ReplaceTrack_StreamContinuity verifies that ReplaceTrack
// keeps the RTP stream of the sender's SSRC continuous: sequence numbers
// increment by one and timestamps advance by the sample duration across
// source switches (including codec changes and ReplaceTrack(nil) pauses),
// and that a failed replace rolls back without disturbing the original stream.
func Test_RTPSender_ReplaceTrack_StreamContinuity(t *testing.T) { //nolint:cyclop
	lim := test.TimeOut(time.Second * 30)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	s := SettingEngine{}
	s.DisableSRTPReplayProtection(true)

	sender, receiver, err := NewAPI(WithSettingEngine(s)).newPair(Configuration{})
	assert.NoError(t, err)

	trackA, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	rtpSender, err := sender.AddTrack(trackA)
	assert.NoError(t, err)

	packetCh := make(chan *rtp.Packet, 100)
	receiver.OnTrack(func(track *TrackRemote, _ *RTPReceiver) {
		for {
			pkt, _, readErr := track.ReadRTP()
			if readErr != nil {
				return
			}
			packetCh <- pkt
		}
	})

	peerConnectionsConnected := untilConnectionState(PeerConnectionStateConnected, sender, receiver)
	assert.NoError(t, signalPair(sender, receiver))
	<-peerConnectionsConnected

	readPacket := func() *rtp.Packet {
		t.Helper()
		select {
		case pkt := <-packetCh:
			return pkt
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for RTP packet")

			return nil
		}
	}

	const sampleDuration = 20 * time.Millisecond
	const sampleTicks = 1800 // 20ms at 90kHz

	writeSample := func(track *TrackLocalStaticSample) {
		t.Helper()
		assert.NoError(t, track.WriteSample(media.Sample{Data: []byte{0x00, 0x01}, Duration: sampleDuration}))
	}

	newVideoTrack := func(mimeType string) *TrackLocalStaticSample {
		t.Helper()
		track, trackErr := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: mimeType}, "video", "pion")
		assert.NoError(t, trackErr)

		return track
	}

	// Baseline: two packets from the original VP8 source.
	writeSample(trackA)
	pktA1 := readPacket()
	writeSample(trackA)
	pktA2 := readPacket()
	assert.Equal(t, pktA1.SequenceNumber+1, pktA2.SequenceNumber)
	assert.Equal(t, pktA1.Timestamp+sampleTicks, pktA2.Timestamp)

	ssrc := pktA1.SSRC
	vp8PT := pktA1.PayloadType

	// VP8 -> VP8: the new source continues the stream.
	trackB := newVideoTrack(MimeTypeVP8)
	assert.NoError(t, rtpSender.ReplaceTrack(trackB))
	writeSample(trackB)
	pktB := readPacket()
	assert.Equal(t, pktA2.SequenceNumber+1, pktB.SequenceNumber)
	assert.Equal(t, pktA2.Timestamp+sampleTicks, pktB.Timestamp)
	assert.Equal(t, ssrc, pktB.SSRC)
	assert.Equal(t, vp8PT, pktB.PayloadType)

	// VP8 -> H264: still continuous, and the receiver can tell the new codec
	// apart by the negotiated payload type.
	trackC := newVideoTrack(MimeTypeH264)
	assert.NoError(t, rtpSender.ReplaceTrack(trackC))
	writeSample(trackC)
	pktC := readPacket()
	assert.Equal(t, pktB.SequenceNumber+1, pktC.SequenceNumber)
	assert.Equal(t, pktB.Timestamp+sampleTicks, pktC.Timestamp)
	assert.Equal(t, ssrc, pktC.SSRC)
	assert.NotEqual(t, vp8PT, pktC.PayloadType)

	var h264PT uint8
	for _, codec := range sender.api.mediaEngine.getCodecsByKind(RTPCodecTypeVideo) {
		if codec.MimeType == MimeTypeH264 {
			h264PT = uint8(codec.PayloadType)

			break
		}
	}
	assert.Equal(t, h264PT, pktC.PayloadType)

	// ReplaceTrack(nil) pauses the stream; samples written to the unbound
	// track never reach the network and must not move the resume position.
	assert.NoError(t, rtpSender.ReplaceTrack(nil))
	assert.NoError(t, trackC.WriteSample(media.Sample{Data: []byte{0x00, 0x01}, Duration: sampleDuration}))

	trackD := newVideoTrack(MimeTypeVP8)
	assert.NoError(t, rtpSender.ReplaceTrack(trackD))
	writeSample(trackD)
	pktD := readPacket()
	assert.Equal(t, pktC.SequenceNumber+1, pktD.SequenceNumber)
	assert.Equal(t, pktC.Timestamp+sampleTicks, pktD.Timestamp)
	assert.Equal(t, ssrc, pktD.SSRC)
	assert.Equal(t, vp8PT, pktD.PayloadType)

	// A failed replace rolls back: the original track keeps streaming and its
	// sequence numbers and timestamps are unaffected.
	assert.True(t, errors.Is(rtpSender.ReplaceTrack(&errBindTrack{}), errBindTrackBind))
	writeSample(trackD)
	pktD2 := readPacket()
	assert.Equal(t, pktD.SequenceNumber+1, pktD2.SequenceNumber)
	assert.Equal(t, pktD.Timestamp+sampleTicks, pktD2.Timestamp)
	assert.Equal(t, ssrc, pktD2.SSRC)

	closePairNow(t, sender, receiver)
}

func Test_RTPSender_Send(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	rtpSender, err := peerConnection.AddTrack(track)
	assert.NoError(t, err)

	parameter := rtpSender.GetParameters()
	err = rtpSender.Send(parameter)
	<-rtpSender.sendCalled
	assert.NoError(t, err)

	assert.NoError(t, peerConnection.Close())
}

func Test_RTPSender_Send_Called_Once(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	rtpSender, err := peerConnection.AddTrack(track)
	assert.NoError(t, err)

	parameter := rtpSender.GetParameters()
	err = rtpSender.Send(parameter)
	<-rtpSender.sendCalled
	assert.NoError(t, err)

	err = rtpSender.Send(parameter)
	assert.Equal(t, errRTPSenderSendAlreadyCalled, err)

	assert.NoError(t, peerConnection.Close())
}

func Test_RTPSender_Send_Track_Removed(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	rtpSender, err := peerConnection.AddTrack(track)
	assert.NoError(t, err)

	parameter := rtpSender.GetParameters()
	assert.NoError(t, peerConnection.RemoveTrack(rtpSender))
	assert.Equal(t, errRTPSenderTrackRemoved, rtpSender.Send(parameter))

	assert.NoError(t, peerConnection.Close())
}

func Test_RTPSender_Add_Encoding(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	rtpSender, err := peerConnection.AddTrack(track)
	assert.NoError(t, err)

	assert.Equal(t, errRTPSenderTrackNil, rtpSender.AddEncoding(nil))

	track1, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderRidNil, rtpSender.AddEncoding(track1))

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("h"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderNoBaseEncoding, rtpSender.AddEncoding(track1))

	track, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("q"))
	assert.NoError(t, err)

	rtpSender, err = peerConnection.AddTrack(track)
	assert.NoError(t, err)

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video1", "pion", WithRTPStreamID("h"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderBaseEncodingMismatch, rtpSender.AddEncoding(track1))

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion1", WithRTPStreamID("h"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderBaseEncodingMismatch, rtpSender.AddEncoding(track1))

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeOpus}, "video", "pion", WithRTPStreamID("h"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderBaseEncodingMismatch, rtpSender.AddEncoding(track1))

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("q"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderRIDCollision, rtpSender.AddEncoding(track1))

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("h"))
	assert.NoError(t, err)
	assert.NoError(t, rtpSender.AddEncoding(track1))

	err = rtpSender.Send(rtpSender.GetParameters())
	assert.NoError(t, err)

	track1, err = NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion", WithRTPStreamID("f"))
	assert.NoError(t, err)
	assert.Equal(t, errRTPSenderSendAlreadyCalled, rtpSender.AddEncoding(track1))

	err = rtpSender.Stop()
	assert.NoError(t, err)

	assert.Equal(t, errRTPSenderStopped, rtpSender.AddEncoding(track1))

	assert.NoError(t, peerConnection.Close())
}

// nolint: dupl
func Test_RTPSender_FEC_Support(t *testing.T) {
	t.Run("FEC disabled by default", func(t *testing.T) {
		track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
		assert.NoError(t, err)

		peerConnection, err := NewPeerConnection(Configuration{})
		assert.NoError(t, err)

		rtpSender, err := peerConnection.AddTrack(track)
		assert.NoError(t, err)

		assert.Zero(t, rtpSender.GetParameters().Encodings[0].FEC.SSRC)
		assert.NoError(t, peerConnection.Close())
	})

	t.Run("FEC can be enabled", func(t *testing.T) {
		mediaEngine := MediaEngine{}
		assert.NoError(t, mediaEngine.RegisterCodec(RTPCodecParameters{RTPCodecCapability: RTPCodecCapability{MimeTypeVP8, 90000, 0, "", nil}, PayloadType: 94}, RTPCodecTypeVideo))
		assert.NoError(t, mediaEngine.RegisterCodec(RTPCodecParameters{RTPCodecCapability: RTPCodecCapability{MimeTypeFlexFEC, 90000, 0, "", nil}, PayloadType: 95}, RTPCodecTypeVideo))

		api := NewAPI(WithMediaEngine(&mediaEngine))

		track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
		assert.NoError(t, err)

		peerConnection, err := api.NewPeerConnection(Configuration{})
		assert.NoError(t, err)

		rtpSender, err := peerConnection.AddTrack(track)
		assert.NoError(t, err)

		assert.NotZero(t, rtpSender.GetParameters().Encodings[0].FEC.SSRC)
		assert.NoError(t, peerConnection.Close())
	})
}

// nolint: dupl
func Test_RTPSender_RTX_Support(t *testing.T) {
	t.Run("RTX SSRC by Default", func(t *testing.T) {
		track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
		assert.NoError(t, err)

		peerConnection, err := NewPeerConnection(Configuration{})
		assert.NoError(t, err)

		rtpSender, err := peerConnection.AddTrack(track)
		assert.NoError(t, err)

		assert.NotZero(t, rtpSender.GetParameters().Encodings[0].RTX.SSRC)
		assert.NoError(t, peerConnection.Close())
	})

	t.Run("RTX can be disabled", func(t *testing.T) {
		mediaEngine := MediaEngine{}
		assert.NoError(t, mediaEngine.RegisterCodec(RTPCodecParameters{RTPCodecCapability: RTPCodecCapability{MimeTypeVP8, 90000, 0, "", nil}, PayloadType: 94}, RTPCodecTypeVideo))
		api := NewAPI(WithMediaEngine(&mediaEngine))

		track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
		assert.NoError(t, err)

		peerConnection, err := api.NewPeerConnection(Configuration{})
		assert.NoError(t, err)

		rtpSender, err := peerConnection.AddTrack(track)
		assert.NoError(t, err)

		assert.Zero(t, rtpSender.GetParameters().Encodings[0].RTX.SSRC)

		assert.NoError(t, peerConnection.Close())
	})
}

type TrackLocalCheckRTCPReaderOnBind struct {
	*TrackLocalStaticSample
	t          *testing.T
	bindCalled chan struct{}
}

func (s *TrackLocalCheckRTCPReaderOnBind) Bind(ctx TrackLocalContext) (RTPCodecParameters, error) {
	assert.NotNil(s.t, ctx.RTCPReader())
	p, err := s.TrackLocalStaticSample.Bind(ctx)
	close(s.bindCalled)

	return p, err
}

func Test_RTPSender_RTCPReader_Bind_Not_Nil(t *testing.T) {
	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	peerConnection, err := NewPeerConnection(Configuration{})
	assert.NoError(t, err)

	bindCalled := make(chan struct{})
	rtpSender, err := peerConnection.AddTrack(&TrackLocalCheckRTCPReaderOnBind{t: t, TrackLocalStaticSample: track, bindCalled: bindCalled})
	assert.NoError(t, err)

	parameter := rtpSender.GetParameters()
	err = rtpSender.Send(parameter)
	<-rtpSender.sendCalled
	<-bindCalled
	assert.NoError(t, err)

	assert.NoError(t, peerConnection.Close())
}

func Test_RTPSender_SetReadDeadline_Crash(t *testing.T) {
	stackA, stackB, err := newORTCPair()
	assert.NoError(t, err)

	assert.NoError(t, signalORTCPair(stackA, stackB))

	track, err := NewTrackLocalStaticSample(RTPCodecCapability{MimeType: MimeTypeVP8}, "video", "pion")
	assert.NoError(t, err)

	rtpSender, err := stackA.api.NewRTPSender(track, stackA.dtls)
	assert.NoError(t, err)

	assert.Error(t, rtpSender.SetReadDeadline(time.Time{}), errRTPSenderSendNotCalled)
	assert.NoError(t, stackA.close())
	assert.NoError(t, stackB.close())
}
