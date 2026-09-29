# Box OCR DeepStream Pipeline

This project is a NVIDIA DeepStream/GStreamer video pipeline for processing one
or more camera streams. It uses a YOLO detector, object tracking, and
DeepStream analytics to identify objects and track region-of-interest (ROI) and
line-crossing events. For objects reported inside the ROI, the pipeline crops
the object image and passes it to an OCR integration. The pipeline also serves
its composed video as an RTSP stream.

## Repository contents

- `deepstream_template.py` — builds and runs the DeepStream pipeline.
- `client.py` — synchronous Triton gRPC inference helper and a small standalone
  example.
- `debug_config.json` — camera stream links and the default RTSP service name.
- `models/Primary_Detector/` — detector configuration, YOLO model files,
  TensorRT-related assets, tracker and analytics configuration, and a
  custom YOLO DeepStream inference plugin.
- `models/Primary_Detector/paddle_model/` — Paddle OCR model files and
  TensorRT caches.
- `common/` — DeepStream support utilities.
- `Dockerfile` and `requirements.txt` — container definition and Python
  package pins.

## Requirements

- An NVIDIA GPU and NVIDIA Container Toolkit configured for Docker.
- A compatible NVIDIA driver and NVIDIA DeepStream 6.0.1 runtime. The supplied
  Dockerfile is based on `nvcr.io/nvidia/deepstream:6.0.1-base`.
- Network access to each configured video source.
- A working OCR service/client integration before running the full pipeline;
  see [Current integration requirements](#current-integration-requirements).

The custom inference plugin and TensorRT engine/cache files are platform- and
runtime-sensitive. Rebuild or regenerate them if the target GPU, CUDA,
TensorRT, or DeepStream environment differs from the one they were prepared
for.

## Configure video sources

Edit `debug_config.json`. `data` must be a mapping from camera identifiers to
entries containing a GStreamer-compatible URI in `link`:

```json
{
  "service_id": "box_detector",
  "data": {
    "camera-1": {
      "link": "rtsp://camera-host:554/path"
    },
    "camera-2": {
      "link": "file:///absolute/path/to/video.mp4"
    }
  }
}
```

Use a reachable camera URI or an absolute `file://` URI. `service_id` is used
as the RTSP mount path and can be overridden with the `SERVICE_ID` environment
variable. The pipeline loads this JSON from the current working directory.

Detector, tracker, and analytics settings are under
`models/Primary_Detector/`. In particular, `config_nvdsanalytics.txt` contains
the ROI and line-crossing configuration. The current ROI update logic also
checks for `images/roi.json` at startup; when present, it uses that file to
update the analytics coordinates and then removes it.

## Build the container

Run from the repository root:

```bash
docker build -t box-ocr-deepstream .
```

## Runtime endpoints

When the pipeline is running, it is configured to serve RTSP on port `8554`,
with the mount path set to the service ID. For the default service ID, the
reported URL is:

```text
rtsp://localhost:8554/box_detector
```
