FROM quay.io/coreos-assembler/fcos-buildroot:testing-devel as build
COPY . /srv/src
RUN cd /srv/src && cargo build --release

FROM registry.fedoraproject.org/fedora:34
COPY --from=build /srv/src/target/release/coreos-diskimage-rehydrator /usr/bin/
COPY --from=build /srv/src/build.sh /root
RUN /root/build.sh && rm -f /root/build.sh
WORKDIR /srv
ENTRYPOINT ["/usr/bin/coreos-diskimage-rehydrator"]
