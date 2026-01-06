package sever

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/log"
	"github.com/gofiber/websocket/v2"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// --- Global Variables ---
var (
	listLock        sync.RWMutex
	gameConnections = map[string]map[string][]peerConnectionState{}

	// 🔹 Modified: now stores username + track
	gameTracks            = map[string]map[string]map[string]userTrack{}
	trackToPeerConnection = make(map[string]map[string]map[*webrtc.TrackLocalStaticRTP]bool)
	mapForPeerConnection  = make(map[*webrtc.PeerConnection]string)
	mapUnitIdGameId       = make(map[string]map[string]bool)

	mapForRangeTrack = map[string]map[string]map[string]float64{
		"28": {
			"48": {
				"50": 42709.92110597 / 1852.0,
				"51": 64125.19222566 / 1852.0,
				"53": 181408.04389492 / 1852.0,
				"49": 32651.19164479 / 1852.0,
				"43": 899450.64926431 / 1852.0,
				"46": 55754.57634378 / 1852.0,
				"47": 27658.0377358 / 1852.0,
				"35": 61145.41164403 / 1852.0,
			},
			"50": {
				"48": 42709.92110597 / 1852.0,
				"51": 27658.58478143 / 1852.0,
				"53": 142124.148728 / 1852.0,
				"49": 27658.0377358 / 1852.0,
				"43": 857105.00882059 / 1852.0,
				"46": 41229.44511605 / 1852.0,
				"47": 32620.37414593 / 1852.0,
				"35": 59510.11516945 / 1852.0,
			},
			"51": {
				"48": 64125.19222566 / 1852.0,
				"50": 27658.58478143 / 1852.0,
				"53": 138135.1311162 / 1852.0,
				"49": 55316.62248911 / 1852.0,
				"43": 842579.9952343 / 1852.0,
				"46": 30112.88309242 / 1852.0,
				"47": 42685.14653153 / 1852.0,
				"35": 85848.29285564 / 1852.0,
			},
			"53": {
				"48": 181408.04389492 / 1852.0,
				"50": 142124.148728 / 1852.0,
				"51": 138135.1311162 / 1852.0,
				"49": 151153.58580122 / 1852.0,
				"43": 723823.69014455 / 1852.0,
				"46": 168198.82152102 / 1852.0,
				"47": 173945.54487051 / 1852.0,
				"35": 146624.2346697 / 1852.0,
			},
			"49": {
				"48": 32651.19164479 / 1852.0,
				"50": 27658.0377358 / 1852.0,
				"51": 55316.62248911 / 1852.0,
				"53": 151153.58580122 / 1852.0,
				"43": 872262.77707248 / 1852.0,
				"46": 63425.26685323 / 1852.0,
				"47": 42848.47295285 / 1852.0,
				"35": 35257.25502201 / 1852.0,
			},
			"43": {
				"48": 899450.64926431 / 1852.0,
				"50": 857105.00882059 / 1852.0,
				"51": 842579.9952343 / 1852.0,
				"53": 723823.69014455 / 1852.0,
				"46": 868302.70124905 / 1852.0,
				"47": 884733.42326955 / 1852.0,
				"35": 870403.64156724 / 1852.0,
			},
			"46": {
				"48": 55754.57634378 / 1852.0,
				"50": 41229.44511605 / 1852.0,
				"51": 30112.88309242 / 1852.0,
				"53": 168198.82152102 / 1852.0,
				"49": 63425.26685323 / 1852.0,
				"43": 868302.70124905 / 1852.0,
				"47": 28150.71543368 / 1852.0,
				"35": 98349.87735369 / 1852.0,
			},
			"47": {
				"48": 27658.0377358 / 1852.0,
				"50": 32620.37414593 / 1852.0,
				"51": 42685.14653153 / 1852.0,
				"53": 173945.54487051 / 1852.0,
				"49": 42848.47295285 / 1852.0,
				"43": 884733.42326955 / 1852.0,
				"46": 28150.71543368 / 1852.0,
				"35": 77722.20954862 / 1852.0,
			},
			"35": {
				"48": 61145.41164403 / 1852.0,
				"50": 59510.11516945 / 1852.0,
				"51": 85848.29285564 / 1852.0,
				"53": 146624.2346697 / 1852.0,
				"49": 35257.25502201 / 1852.0,
				"43": 870403.64156724 / 1852.0,
				"46": 98349.87735369 / 1852.0,
				"47": 77722.20954862 / 1852.0,
			},
		},
	}
)

// 🔹 Added struct for track info
type userTrack struct {
	Username   string
	Track      *webrtc.TrackLocalStaticRTP
	unitID     string
	teamId     string
	Incryption string
	dummyTrack *webrtc.TrackLocalStaticRTP
	dummyDone  chan struct{}
}

// --- Structs ---
type websocketMessage struct {
	Event string `json:"event"`
	Data  string `json:"data"`
	User  string `json:"user,omitempty"` // 🔹 Added: who the message came from
}

type peerConnectionState struct {
	peerConnection *webrtc.PeerConnection
	websocket      *threadSafeWriter
	unitID         string
	teamId         string
	radioRange     float64
	Username       string
}

// --- Peer Connection Helpers ---

// 🔹 Updated: addTrack now takes username
func CreateDummyAudioTrack(
	codec webrtc.RTPCodecCapability,
	t *webrtc.TrackRemote,
	done <-chan struct{},
) *webrtc.TrackLocalStaticRTP {

	dummyTrack, err := webrtc.NewTrackLocalStaticRTP(
		codec,
		t.ID(),       // <-- YOU WANT THIS
		t.StreamID(), // <-- KEEP SAME
	)
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		seq := uint16(rand.Intn(65535))
		timestamp := uint32(time.Now().UnixNano() / 1e6)
		payload := make([]byte, 200)

		for {
			select {
			case <-done:
				return

			case <-ticker.C:
				for i := range payload {
					payload[i] = byte(rand.Intn(256))
				}

				packet := &rtp.Packet{
					Header: rtp.Header{
						Version:        2,
						PayloadType:    uint8(codec.ClockRate),
						SequenceNumber: seq,
						Timestamp:      timestamp,
						SSRC:           uint32(rand.Intn(999999)),
					},
					Payload: payload,
				}

				if err := dummyTrack.WriteRTP(packet); err != nil {
					return
				}

				seq++
				timestamp += 960
			}
		}
	}()

	return dummyTrack
}

func addTrack(gameID, teamID, username, unitID string,
	t *webrtc.TrackRemote,
	pc *webrtc.PeerConnection,
	radioRange float64,
	encryption string) *webrtc.TrackLocalStaticRTP {

	listLock.Lock()
	defer func() {
		listLock.Unlock()
		signalPeerConnectionsForGameTeam(gameID, teamID, username)
	}()

	if _, ok := gameTracks[gameID]; !ok {
		gameTracks[gameID] = map[string]map[string]userTrack{}
	}
	if _, ok := gameTracks[gameID][teamID]; !ok {
		gameTracks[gameID][teamID] = map[string]userTrack{}
	}

	if _, ok := trackToPeerConnection[gameID]; !ok {
		trackToPeerConnection[gameID] = make(map[string]map[*webrtc.TrackLocalStaticRTP]bool)
	}
	if _, ok := trackToPeerConnection[gameID][teamID]; !ok {
		trackToPeerConnection[gameID][teamID] = make(map[*webrtc.TrackLocalStaticRTP]bool)
	}

	trackLocal, err := webrtc.NewTrackLocalStaticRTP(
		t.Codec().RTPCodecCapability,
		t.ID(),
		t.StreamID(),
	)
	if err != nil {
		panic(err)
	}

	ut := userTrack{
		Username:   username,
		Track:      trackLocal,
		unitID:     unitID,
		Incryption: encryption,
		teamId:     teamID,
	}

	// Dummy logic (same ID as real)
	if encryption == "1" {
		done := make(chan struct{})
		dummyTrack := CreateDummyAudioTrack(t.Codec().RTPCodecCapability, t, done)

		fmt.Println("Dummy track id", dummyTrack.ID())

		ut.dummyTrack = dummyTrack
		ut.dummyDone = done
	}

	// store only ONE entry: real ID
	gameTracks[gameID][teamID][t.ID()] = ut

	fmt.Println("t ID from the adding track function", t.ID())

	trackToPeerConnection[gameID][teamID][trackLocal] = false

	return trackLocal
}

// 🔹 Updated: removeTrack still works the same, only map type changed

func removeTrack(gameID, teamID string, t *webrtc.TrackLocalStaticRTP, username string) {
	listLock.Lock()
	defer listLock.Unlock()

	ut, ok := gameTracks[gameID][teamID][t.ID()]
	if !ok {
		return
	}

	// ✅ Stop dummy track if exists
	if ut.dummyDone != nil {
		close(ut.dummyDone) // This signals the dummy track goroutine to exit
	}

	delete(gameTracks[gameID][teamID], t.ID())
	// Also remove from trackToPeerConnection if needed
}

//	func removeTrack(gameID, teamID string, t *webrtc.TrackLocalStaticRTP, username string) {
//		listLock.Lock()
//		defer func() {
//			listLock.Unlock()
//			signalPeerConnectionsForGameTeam(gameID, teamID, username)
//		}()
//		delete(gameTracks[gameID][teamID], t.ID())
//	}
func signalPeerConnectionsForGameTeam(gameID, teamID, username string) {
	fmt.Println("[INFO] Start process for:", username)

	attemptSync := func() (tryAgain bool) {
		listLock.Lock()
		pcs1 := gameConnections[gameID]
		tracks1 := gameTracks[gameID] // map[teamID]map[trackID]trackInfo
		listLock.Unlock()

		for team, pcs := range pcs1 {
			for i := 0; i < len(pcs); i++ {

				pc := pcs[i].peerConnection
				ws := pcs[i].websocket

				fmt.Printf("[INFO] Processing PC index %d for user %s\n", i, pcs[i].Username)

				if pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
					fmt.Println("[WARN] PeerConnection closed → removing")
					listLock.Lock()

					pcs = append(pcs[:i], pcs[i+1:]...)
					gameConnections[gameID][team] = pcs
					listLock.Unlock()

					return true // retry needed
				}

				// ========= Existing tracks =========
				existing := map[string]bool{}
				for _, sender := range pc.GetSenders() {
					if sender.Track() != nil {
						existing[sender.Track().ID()] = true
					}
				}
				for _, receiver := range pc.GetReceivers() {
					if receiver.Track() != nil {
						existing[receiver.Track().ID()] = true
					}
				}

				// ========= Cleanup old tracks =========
				for _, sender := range pc.GetSenders() {
					tr := sender.Track()
					if tr == nil {
						continue
					}

					trackFound := false
					var remoteUnitID string
					userName := username
					listLock.Lock()
					//	fmt.Println("The list of track in trun of peer connection : in remove track", pcs[i].Username, tracks1, " With Size ->", len(tracks1))
					for _, allTracks := range tracks1 {
						fmt.Println("tr ID", tr.ID())
						if info, ok := allTracks[tr.ID()]; ok {
							trackFound = true
							remoteUnitID = info.unitID
							userName = info.Username
							break
						}
					}
					listLock.Unlock()

					if !trackFound || mapForRangeTrack[gameID][pcs[i].unitID][remoteUnitID] > pcs[i].radioRange {
						fmt.Println("[INFO] Removing track:", tr.ID(), sender)

						err := pc.RemoveTrack(sender)
						if err != nil {
							fmt.Println("Got err add the time of remove al track% :", err)
						}
						// / ========= Cleanup old tracks =========
						for _, sender1 := range pc.GetSenders() {
							if sender == sender1 {
								fmt.Println("We find this")
							}
						}
						ws.WriteJSON(&websocketMessage{
							Event: "removeTrack",
							Data:  "",
							User:  userName,
						})

						// return true // retry after removing track
					}
				}

				// ========= Add missing tracks =========
				newUsers := map[string]bool{}
				listLock.Lock()
				// fmt.Println("The list of track in trun of peer connection : in adding track", pcs[i].Username, tracks1, " With Size ->", len(tracks1))
				for _, tracks := range tracks1 {
					for trackID, trackInfo := range tracks {

						if existing[trackID] {
							continue
						}

						if mapForRangeTrack[gameID][pcs[i].unitID][trackInfo.unitID] > pcs[i].radioRange {
							continue
						}

						var trackToAdd *webrtc.TrackLocalStaticRTP

						if trackInfo.Incryption == "1" && trackInfo.teamId != pcs[i].teamId {
							// Add dummy track instead of skipping real track from other team
							if trackInfo.dummyTrack != nil {
								trackToAdd = trackInfo.dummyTrack
								fmt.Printf("[INFO] Adding dummy track %s for user %s (encryption on, different team)\n", trackID, pcs[i].Username)
							}
							// } else {
							// 	// fallback: create a new dummy track
							// 	done := make(chan struct{})
							// 	dummy := CreateDummyAudioTrack(trackInfo.Track.Codec(), done)
							// 	trackInfo.dummyTrack = dummy
							// 	trackInfo.dummyDone = done
							// 	trackToAdd = dummy
							// 	fmt.Printf("[INFO] Created and adding new dummy track %s for user %s\n", trackID, pcs[i].Username)
							// }
						} else {
							// normal track
							trackToAdd = trackInfo.Track
							fmt.Printf("[INFO] Adding real track %s (%s) to PC of %s\n", trackID, trackInfo.Username, pcs[i].Username)
						}

						if _, err := pc.AddTrack(trackToAdd); err != nil {
							fmt.Println("perr user name :", pcs[i].Username, "track user name", trackInfo.Username, trackID)
							listLock.Unlock()
							fmt.Println("[ERROR] AddTrack failed:", err)
							return true
						}
						newUsers[trackInfo.Username] = true
					}
				}
				listLock.Unlock()

				if val := mapUnitIdGameId[gameID][pcs[i].unitID]; !val {
					newUsers[pcs[i].Username] = true
					mapUnitIdGameId[gameID][pcs[i].unitID] = true
				}
				fmt.Println(newUsers)
				if len(newUsers) > 0 {
					offer, err := pc.CreateOffer(nil)
					if err != nil {
						fmt.Println("[ERROR] CreateOffer:", err)
						return true
					}
					if err := pc.SetLocalDescription(offer); err != nil {
						fmt.Println("[ERROR] SetLocalDescription:", err)
						// return true
					}
					offerJSON, _ := json.Marshal(offer)

					for remoteUser := range newUsers {
						err := ws.WriteJSON(&websocketMessage{
							Event: "offer",
							Data:  string(offerJSON),
							User:  remoteUser,
						})
						if err != nil {
							fmt.Println("[ERROR] WS send offer failed:", err)
							return true
						}
					}
				}
			}
		}

		return false
	}

	// ===== Retry loop =====
	for syncAttempt := 0; ; syncAttempt++ {
		fmt.Printf("[INFO] Sync attempt #%d for %s\n", syncAttempt+1, username)

		if syncAttempt == 25 {
			fmt.Println("[WARN] Max sync attempts reached. Retrying in 3 seconds...")
			go func() {
				time.Sleep(3 * time.Second)
				signalPeerConnectionsForGameTeam(gameID, teamID, username)
			}()
			return
		}

		if !attemptSync() {
			fmt.Println("[INFO] Sync successful for user:", username)
			break
		} else {
			fmt.Println("[INFO] Retry required due to previous error or closed connection")
		}
	}

	fmt.Println("[INFO] Completed signaling for user:", username)
}

func DispatchAllKeyFrames() {
	listLock.Lock()
	defer listLock.Unlock()

	for _, teamMap := range gameConnections {
		for _, pcs := range teamMap {
			for _, pcState := range pcs {
				for _, receiver := range pcState.peerConnection.GetReceivers() {
					if receiver.Track() != nil {
						_ = pcState.peerConnection.WriteRTCP([]rtcp.Packet{
							&rtcp.PictureLossIndication{MediaSSRC: uint32(receiver.Track().SSRC())},
						})
					}
				}
			}
		}
	}
}

// --- Fiber WebSocket Handler ---

func WebsocketHandler(c *websocket.Conn) {

	fmt.Println("serve is called")

	gameID := c.Query("gameID")
	teamID := c.Query("teamID")
	username := c.Query("username") // 🔹 Added username from query
	unitID := c.Query("unitID")
	encryption := c.Query("encyptionOn")
	fmt.Println(encryption)
	radioRange, err := strconv.ParseFloat(c.Query("radioRange"), 64)
	//fmt.Println(mapForRangeTrack)
	fmt.Println("Default Range", radioRange)

	if err != nil {
		c.WriteMessage(websocket.TextMessage, []byte("Error in conversion of radio range"))
		c.Close()
		return
	}

	if gameID == "" || teamID == "" {
		c.WriteMessage(websocket.TextMessage, []byte("gameID and teamID required"))
		c.Close()
		return
	}

	tsWriter := &threadSafeWriter{Conn: c}

	settingEngine := webrtc.SettingEngine{}
	// Optional: force TURN in production if UDP blocked
	settingEngine.SetICETransportPolicy(webrtc.ICETransportPolicyAll)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			// STUN for local testing
			{
				URLs: []string{
					"stun:stun.l.google.com:19302",
					"stun:stun1.l.google.com:19302",
				},
			},
			// TURN for production/cloud
			{
				URLs: []string{
					"turn:global.relay.metered.ca:80?transport=tcp",
					"turns:global.relay.metered.ca:443",
				},
				Username:   "fd09f21aad0c834cb5c69447",
				Credential: "VsUmB01i5+KwCDPN",
			},
		},
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		log.Errorf("Failed to create PeerConnection: %v", err)
		return
	}
	defer pc.Close()
	mapForPeerConnection[pc] = username
	for _, typ := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio} {
		pc.AddTransceiverFromKind(typ, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		})
	}

	listLock.Lock()
	if _, ok := mapUnitIdGameId[gameID]; !ok {

		mapUnitIdGameId[gameID] = make(map[string]bool)
	}
	mapUnitIdGameId[gameID][username] = false
	if _, ok := gameConnections[gameID]; !ok {
		gameConnections[gameID] = map[string][]peerConnectionState{}
	}
	gameConnections[gameID][teamID] = append(gameConnections[gameID][teamID], peerConnectionState{pc, tsWriter, unitID, teamID, radioRange, username})
	listLock.Unlock()

	pc.OnICECandidate(func(i *webrtc.ICECandidate) {
		if i == nil {
			return
		}
		candidateJSON, err := json.Marshal(i.ToJSON())
		if err == nil {
			tsWriter.WriteJSON(&websocketMessage{
				Event: "candidate",
				Data:  string(candidateJSON),
				User:  username, // 🔹 send username with ICE candidate too
			})
		}
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Infof("Connection state: %s", s)
		switch s {
		case webrtc.PeerConnectionStateFailed:
			pc.Close()
		case webrtc.PeerConnectionStateClosed:
			signalPeerConnectionsForGameTeam(gameID, teamID, username)
		}
	})

	pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		log.Infof("Got remote track from user %s: Kind=%s, ID=%s", username, t.Kind(), t.ID())
		trackLocal := addTrack(gameID, teamID, username, unitID, t, pc, radioRange, encryption) // 🔹 Pass username
		defer removeTrack(gameID, teamID, trackLocal, username)

		buf := make([]byte, 1500)
		rtpPkt := &rtp.Packet{}
		for {
			n, _, err := t.Read(buf)
			if err != nil {
				return
			}
			if err := rtpPkt.Unmarshal(buf[:n]); err != nil {
				log.Errorf("Failed to unmarshal RTP: %v", err)
				return
			}
			rtpPkt.Extension = false
			rtpPkt.Extensions = nil
			if err := trackLocal.WriteRTP(rtpPkt); err != nil {
				return
			}
		}
	})

	signalPeerConnectionsForGameTeam(gameID, teamID, username)

	message := &websocketMessage{}
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			log.Errorf("Read message failed: %v", err)
			return
		}

		if err := json.Unmarshal(raw, &message); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch message.Event {
		case "candidate":
			cand := webrtc.ICECandidateInit{}
			if err := json.Unmarshal([]byte(message.Data), &cand); err == nil {
				pc.AddICECandidate(cand)
			}
		case "answer":
			answer := webrtc.SessionDescription{}
			if err := json.Unmarshal([]byte(message.Data), &answer); err == nil {
				pc.SetRemoteDescription(answer)
			}
		default:
			log.Errorf("Unknown message: %+v", message)
		}
	}
}

// --- Thread Safe Writer ---

type threadSafeWriter struct {
	*websocket.Conn
	sync.Mutex
}

func (t *threadSafeWriter) WriteJSON(v any) error {
	t.Lock()
	defer t.Unlock()
	return t.Conn.WriteJSON(v)
}
