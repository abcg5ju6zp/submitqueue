// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/controller"
	controllermock "github.com/uber/submitqueue/stovepipe/controller/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
)

type fakeRequestHistoryController struct {
	getByID  func(context.Context, entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error)
	getByURI func(context.Context, entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error)
}

var _ controller.RequestHistoryController = (*fakeRequestHistoryController)(nil)

func (f *fakeRequestHistoryController) GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error) {
	return f.getByID(ctx, req)
}

func (f *fakeRequestHistoryController) GetRequestHistoryByURI(ctx context.Context, req entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error) {
	return f.getByURI(ctx, req)
}

func TestGetRequestHistoryByID(t *testing.T) {
	controllerErr := errors.New("controller failed")
	logs := []entity.RequestLog{
		{ID: "occurrence/1", State: entity.RequestStateAccepted, TimestampMs: 1000},
		{ID: "occurrence/2", Event: entity.RequestEventBuildTriggered, TimestampMs: 2000},
	}
	tests := []struct {
		name     string
		logs     []entity.RequestLog
		err      error
		wantLogs int
	}{
		{name: "maps successful result", logs: logs, wantLogs: 2},
		{name: "maps empty result", logs: nil, wantLogs: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotReq entity.GetRequestHistoryByIDRequest
			fake := &fakeRequestHistoryController{
				getByID: func(_ context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error) {
					gotReq = req
					return tt.logs, tt.err
				},
			}
			srv := &StovepipeServer{requestHistoryController: fake}

			resp, err := srv.GetRequestHistoryByID(context.Background(), &pb.GetRequestHistoryByIDRequest{
				Queue: "monorepo/main", RequestId: "1",
			})

			assert.Equal(t, entity.GetRequestHistoryByIDRequest{Queue: "monorepo/main", ID: "1"}, gotReq)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Events, tt.wantLogs)
			if tt.wantLogs > 0 {
				assert.Equal(t, "accepted", resp.Events[0].GetRequestState())
				assert.Equal(t, "build_triggered", resp.Events[1].GetEvent())
			}
		})
	}
}

func TestGetRequestHistoryByURI(t *testing.T) {
	controllerErr := errors.New("controller failed")
	histories := []entity.RequestHistory{{
		RequestID: "1",
		Events:    []entity.RequestLog{{ID: "occurrence/1", State: entity.RequestStateAccepted}},
	}}
	tests := []struct {
		name          string
		histories     []entity.RequestHistory
		err           error
		wantHistories int
	}{
		{name: "maps successful result", histories: histories, wantHistories: 1},
		{name: "maps empty result", histories: nil, wantHistories: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotReq entity.GetRequestHistoryByURIRequest
			fake := &fakeRequestHistoryController{
				getByURI: func(_ context.Context, req entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error) {
					gotReq = req
					return tt.histories, tt.err
				},
			}
			srv := &StovepipeServer{requestHistoryController: fake}

			resp, err := srv.GetRequestHistoryByURI(context.Background(), &pb.GetRequestHistoryByURIRequest{
				Queue: "monorepo/main", Uri: "git://monorepo/abc",
			})

			assert.Equal(t, entity.GetRequestHistoryByURIRequest{Queue: "monorepo/main", URI: "git://monorepo/abc"}, gotReq)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Histories, tt.wantHistories)
			if tt.wantHistories > 0 {
				assert.Equal(t, "1", resp.Histories[0].RequestId)
				require.Len(t, resp.Histories[0].Events, 1)
				assert.Equal(t, "accepted", resp.Histories[0].Events[0].GetRequestState())
			}
		})
	}
}

func TestGetProjectStatus(t *testing.T) {
	controllerErr := errors.New("controller failed")
	for _, selector := range []string{"ID", "URI"} {
		for _, callErr := range []error{nil, controllerErr} {
			name := selector + "/success"
			if callErr != nil {
				name = selector + "/controller error"
			}
			t.Run(name, func(t *testing.T) {
				wantReq := entity.GetProjectStatusRequest{Queue: "queue", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token"}
				if selector == "ID" {
					wantReq.RequestID = "7"
				} else {
					wantReq.ChangeURI = "uri"
				}
				projectStatusController := controllermock.NewMockProjectStatusController(gomock.NewController(t))
				projectStatusController.EXPECT().GetProjectStatus(gomock.Any(), wantReq).Return(entity.GetProjectStatusResult{
					RequestSummary: entity.RequestSummary{RequestID: "7", Queue: "queue", URI: "uri", State: entity.RequestStateSucceeded},
				}, callErr)
				srv := &StovepipeServer{projectStatusController: projectStatusController}
				if selector == "ID" {
					response, err := srv.GetProjectStatusByID(context.Background(), &pb.GetProjectStatusByIDRequest{
						Queue: "queue", RequestId: "7", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token",
					})
					if callErr != nil {
						require.ErrorIs(t, err, callErr)
						assert.Nil(t, response)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, "7", response.GetRequestId())
					assert.Equal(t, "succeeded", response.GetRequestState())
				} else {
					response, err := srv.GetProjectStatusByURI(context.Background(), &pb.GetProjectStatusByURIRequest{
						Queue: "queue", ChangeUri: "uri", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token",
					})
					if callErr != nil {
						require.ErrorIs(t, err, callErr)
						assert.Nil(t, response)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, "7", response.GetRequestId())
					assert.Equal(t, "succeeded", response.GetRequestState())
				}
			})
		}
	}
}
