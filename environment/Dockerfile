FROM public.ecr.aws/x8v8d7g8/mars-base:latest
WORKDIR /app
ARG REPO_URL=https://github.com/uber/submitqueue
ARG BASE_SHA=ff1e8cbe33b3001c529632ac8dc0949c14528cd0
RUN git clone "$REPO_URL" . \
 && DEFAULT="$(git remote show origin | sed -n 's/.*HEAD branch: //p')" \
 && git checkout -B "$DEFAULT" "$BASE_SHA" \
 && git remote remove origin \
 && git reflog expire --expire=now --all \
 && git gc --prune=now
RUN go mod download
CMD ["bash"]
