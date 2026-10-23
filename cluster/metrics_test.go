/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package cluster

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func getCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}

	return m.GetCounter().GetValue()
}

func getGaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()

	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("write gauge metric: %v", err)
	}

	return m.GetGauge().GetValue()
}

func TestRecordNotificationSent(t *testing.T) {
	before := getCounterValue(t, NotificationsSentTotal)

	RecordNotificationSent()

	after := getCounterValue(t, NotificationsSentTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordNotificationReceived(t *testing.T) {
	before := getCounterValue(t, NotificationsReceivedTotal)

	RecordNotificationReceived()

	after := getCounterValue(t, NotificationsReceivedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordFetchServed(t *testing.T) {
	before := getCounterValue(t, FetchesServedTotal)

	RecordFetchServed()

	after := getCounterValue(t, FetchesServedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordPeerConnectedDisconnected(t *testing.T) {
	RecordPeerConnected(1)

	g := peerConnected.WithLabelValues("1")

	val := getGaugeValue(t, g)
	if val != 1 {
		t.Fatalf("expected peer_connected=1, got %f", val)
	}

	RecordPeerDisconnected(1)

	val = getGaugeValue(t, g)
	if val != 0 {
		t.Fatalf("expected peer_connected=0, got %f", val)
	}
}

func TestRecordNotificationDropped(t *testing.T) {
	RecordNotificationDropped(5)

	c := peerNotificationsDroppedTotal.WithLabelValues("5")

	if getCounterValue(t, c) < 1 {
		t.Fatal("expected notifications_dropped counter >= 1")
	}
}

func TestRecordPeerReconnect(t *testing.T) {
	RecordPeerReconnect(3)

	c := peerReconnectsTotal.WithLabelValues("3")

	if getCounterValue(t, c) < 1 {
		t.Fatal("expected reconnects counter >= 1")
	}
}
