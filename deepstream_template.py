#!/usr/bin/env python3

################################################################################
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
################################################################################


import gi
gi.require_version('Gst', '1.0')
from gi.repository import GObject, Gst, GstRtspServer, GLib
from kafka import KafkaProducer
import sys
import pyds
from ctypes import *
import ctypes
import configparser
import numpy as np
import datetime
import cv2
import time
import json
import math
import copy
import os
from common.is_aarch_64 import is_aarch64
from common.bus_call import bus_call
from common.utils import long_to_uint64
import logging
import sys
from common.FPS import PERF_DATA

from confluent_kafka import Producer


perf_data = None

formatter = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger("console_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

past_tracking_meta=[0]

tracker_file=os.path.join(os.getcwd(), "models", "Primary_Detector","tracker_config.txt")
nvanalytics_file_path= os.path.join(os.getcwd(), "models", "Primary_Detector", "config_nvdsanalytics.txt")
config_file_path = os.path.join(os.getcwd(), "models", "Primary_Detector", "pgie_config.txt")
config = configparser.ConfigParser()
config.read(config_file_path)
config.sections()
if 'uff-input-dims' in config['property']:
    size=config['property']['uff-input-dims']
    l=size.split(";")
    height=int(l[1])
    width=int(l[2])
    MUXER_OUTPUT_WIDTH=width
    MUXER_OUTPUT_HEIGHT=height
else:
    MUXER_OUTPUT_WIDTH=1920
    MUXER_OUTPUT_HEIGHT=1080
MUXER_BATCH_TIMEOUT_USEC=4000000
TILED_OUTPUT_WIDTH=1920
TILED_OUTPUT_HEIGHT=1080
GST_CAPS_FEATURES_NVMM="memory:NVMM"


debug_info = {}
debug_config_path = os.path.join(os.getcwd(), "debug_config.json")
if os.path.exists(debug_config_path):
    with open(debug_config_path, "r") as f:
        debug_info = json.loads(f.read())


service_id = os.environ.get('SERVICE_ID', debug_info['service_id']) # change the service id of your choice

uri_to_id_mapping = {}
obj_counter = {}
count = 0
date = datetime.datetime.now().strftime("%d-%m-%Y")

buffer_counter = 0
image_buffer_cache_length = 100 # change image buffer size of your choice
image_buffer_cache = {}
image_buffer_cache_key = 0

loop = None
args = None
pipeline = None

producer = Producer(
    {
        "bootstrap.servers": "{}:9094".format("localhost"),
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.username": "admin",
        "sasl.password": "admin-secret",
        "sasl.mechanism": "PLAIN",
    }
)

start_time = time.time()

for key in range(0, image_buffer_cache_length):
    image_buffer_cache[str(key)] = {}

images_folder = os.path.join(os.getcwd(), "images")
if not os.path.exists(os.path.join(images_folder, date)):
    os.makedirs(os.path.join(images_folder, date))

def alytics_cords_update():
    try:
        roi_file=os.path.join(images_folder,"roi.json")
        if os.path.exists(roi_file):
            with open(roi_file,"r") as f:
                content= json.load(f)
                for key in content:
                    roi_data = content[key].get("roi")[0]
                    roix1,roiy1,roix2,roiy2,roix3,roiy3,roix4,roiy4 = roi_data['x1'], roi_data['y1'], roi_data['x2'], roi_data['y2'], roi_data['x3'], roi_data['y3'],roi_data['x4'], roi_data['y4']

                    line_data=content[key].get('line')[0]
                    x1,y1,x2,y2,x3,y3,x4,y4=line_data[0],line_data[1],line_data[2],line_data[3],line_data[4],line_data[5],line_data[6],line_data[7]

            with open(nvanalytics_file_path, 'r') as file:
                config_lines = file.readlines()

            for i, line in enumerate(config_lines):
                if 'roi-RF' in line:
                    roi_rf_index = i
                    break
            new_roi_rf = f"roi-RF={int(roix1)};{int(roiy1)};{int(roix2)};{int(roiy2)};{int(roix3)};{int(roiy3)};{int(roix4)};{int(roiy4)}\n"
            config_lines[roi_rf_index] = new_roi_rf

            with open(nvanalytics_file_path, 'w+') as file:
                file.writelines(config_lines)

            for i, line in enumerate(config_lines):
                if '[line-crossing-stream-0]' in line:
                    line_stream_start = i
                    break

            entry_line_index = None
            for i in range(line_stream_start + 1, len(config_lines)):
                if 'line-crossing-Entry' in config_lines[i]:
                    entry_line_index = i
                    break

            exit_line_index = None
            for i in range(entry_line_index + 1, len(config_lines)):
                if 'line-crossing-Exit' in config_lines[i]:
                    exit_line_index = i
                    break

            new_entry_line = f"line-crossing-Entry={int(x4)};{int(y4)};{int(x3)};{int(y3)};{int(x1)};{int(y1)};{int(x2)};{int(y2)};\n"
            config_lines[entry_line_index] = new_entry_line
            with open(nvanalytics_file_path, 'w+') as file:
                file.writelines(config_lines)


            new_exit_line = f"line-crossing-Exit={int(x3)};{int(y3)};{int(x4)};{int(y4)};{int(x1)};{int(y1)};{int(x2)};{int(y2)};\n"
            config_lines[exit_line_index] = new_exit_line
            with open(nvanalytics_file_path, 'w+') as file:
                file.writelines(config_lines)

            print(config_lines)
            # Write updated line coordinates back to nvanalytics_file_path

            os.remove(roi_file)
            logger.debug(roi_file + "deleted successfully.")
    except Exception as e:
        logger.debug(e)
    
alytics_cords_update()

import client

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
        sys.exit(1)
    else:
        logger.error('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def tiler_sink_pad_buffer_probe(pad, info, u_data):
    global image_buffer_cache_key, buffer_counter, image_buffer_cache_length, image_buffer_cache, start_time,ip
    text=[]
    temp_dict = {}

    camera_meatadata_dictionary = {}
    if 'interval' in config['property']:
        interval=config['property']['interval']
        interval=int(interval)


    if time.time() - start_time > interval:
        image_buffer_cache_key = str(buffer_counter%image_buffer_cache_length)

        gst_buffer = info.get_buffer()
        if not gst_buffer:
            logger.debug("Unable to get GstBuffer ")
            return

        batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
        l_frame = batch_meta.frame_meta_list

        while l_frame is not None:
            try:
                frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
            except StopIteration:
                break

            camera_id = list(uri_to_id_mapping.values())[frame_meta.source_id]
            camera_meatadata_dictionary[camera_id] = copy.deepcopy(obj_counter)
            camera_meatadata_dictionary[camera_id]['buffer_index'] = image_buffer_cache_key

        
            l_obj=frame_meta.obj_meta_list

            while l_obj is not None:

                try: 
                    obj_meta=pyds.NvDsObjectMeta.cast(l_obj.data)
                except StopIteration:
                    break

                l_user_meta = obj_meta.obj_user_meta_list
                while l_user_meta:
                    try:
                        user_meta = pyds.NvDsUserMeta.cast(l_user_meta.data)
                        if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSOBJ.USER_META"):             
                            user_meta_data = pyds.NvDsAnalyticsObjInfo.cast(user_meta.user_meta_data)
                            if user_meta_data.lcStatus: print("Object {0} line crossing status: {1}".format(obj_meta.object_id, user_meta_data.lcStatus))
                            if user_meta_data.roiStatus: print("Object {0} roi status: {1}".format(obj_meta.object_id, user_meta_data.roiStatus))
                    except StopIteration:
                        break

                    try:

                        if user_meta_data.roiStatus:
                            frame_img = pyds.get_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)
                            frame_copy=np.array(frame_img,copy=True,order="C")

                            image_buffer_cache[image_buffer_cache_key].update({camera_id: pyds.get_nvds_buf_surface(hash(gst_buffer),frame_meta.batch_id)})                        

                            frame_copy = cv2.cvtColor(frame_copy, cv2.COLOR_RGBA2BGR)
                            crop_frame_copy= frame_copy[int(obj_meta.rect_params.top) : int(obj_meta.rect_params.top) + int(obj_meta.rect_params.height),
                                                        int(obj_meta.rect_params.left) : int(obj_meta.rect_params.left) + int(obj_meta.rect_params.width)]
                            
                        

                            im =np.array(crop_frame_copy)                        
                            result = client.ppocr_v3.predict(im)

                            camera_meatadata_dictionary[camera_id][obj_meta.class_id]['detections'].append({
                            'object_id': long_to_uint64(obj_meta.object_id),
                            'confidence': obj_meta.confidence,
                            'top': obj_meta.rect_params.top,
                            'left': obj_meta.rect_params.left, 
                            'width': obj_meta.rect_params.width,
                            'height': obj_meta.rect_params.height,
                            'ocr_text': str(result)})

                    except Exception as e:
                        print(e)
                    
                    try:
                        l_user_meta = l_user_meta.next
                    except StopIteration:
                        break
       
                try: 
                    l_obj=l_obj.next
                except StopIteration:
                    break   
            
            l_user = frame_meta.frame_user_meta_list
            while l_user:
                try:
                    user_meta = pyds.NvDsUserMeta.cast(l_user.data)
                    if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSFRAME.USER_META"):
                        user_meta_data = pyds.NvDsAnalyticsFrameMeta.cast(user_meta.user_meta_data)
                        if user_meta_data.objInROIcnt: print("Objs in ROI: {0}".format(user_meta_data.objInROIcnt)) 
                        if user_meta_data.objLCCumCnt: 
                            print("Linecrossing Cumulative: {0}".format(user_meta_data.objLCCumCnt))
                            temp_dict["Linecrossing"] = user_meta_data.objLCCumCnt 
                            camera_meatadata_dictionary[camera_id]['count']=temp_dict

                        if user_meta_data.objLCCurrCnt:print("Linecrossing Current Frame: {0}".format(user_meta_data.objLCCurrCnt))
                            
                            

                except StopIteration:
                    break
                try:
                    l_user = l_user.next
                except StopIteration:
                    break

            stream_index = "stream{0}".format(frame_meta.pad_index)
            global perf_data
            perf_data.update_fps(stream_index)
            perf_data.perf_print_callback()   
             
            try:
                l_frame=l_frame.next
            except StopIteration:
                break
     
            logger.debug(json.dumps({"data": camera_meatadata_dictionary, "room": service_id}, indent=4,default=str))
        if image_buffer_cache_key == str(image_buffer_cache_length-1):
            buffer_counter = 0    
            producer.flush()        
        else:
            buffer_counter += 1
        
        start_time = time.time()
        
    return Gst.PadProbeReturn.OK


def cb_newpad(decodebin, decoder_src_pad,data):
    logger.debug("In cb_newpad\n")

    caps=decoder_src_pad.get_current_caps()
    gststruct=caps.get_structure(0)
    gstname=gststruct.get_name()
    source_bin=data
    features=caps.get_features(0)

    logger.debug("gstname={}".format(gstname))

    if(gstname.find("video")!=-1):
        logger.debug("features={}".format(features))

        if features.contains(GST_CAPS_FEATURES_NVMM):
            bin_ghost_pad=source_bin.get_static_pad("src")
            if not bin_ghost_pad.set_target(decoder_src_pad):
                sys.stderr.write("Failed to link decoder src pad to source bin ghost pad\n")
        else:
            sys.stderr.write(" Error: Decodebin did not pick nvidia decoder plugin.\n")


def decodebin_child_added(child_proxy,Object,name,user_data):
    logger.debug("Decodebin child added:{}\n".format(name))

    if(name.find("decodebin") != -1):
        Object.connect("child-added",decodebin_child_added,user_data) 


def create_source_bin(index,uri):
    logger.debug("Creating source bin")

    bin_name="source-bin-%02d" %index
    logger.debug(bin_name)
    nbin=Gst.Bin.new(bin_name)
    if not nbin:
        sys.stderr.write(" Unable to create source bin \n")

    uri_decode_bin=Gst.ElementFactory.make("uridecodebin", "uri-decode-bin")
    if not uri_decode_bin:
        sys.stderr.write(" Unable to create uri decode bin \n")
    uri_decode_bin.set_property("uri",uri)
    uri_decode_bin.connect("pad-added",cb_newpad,nbin)
    uri_decode_bin.connect("child-added",decodebin_child_added,nbin)
    Gst.Bin.add(nbin,uri_decode_bin)
    bin_pad=nbin.add_pad(Gst.GhostPad.new_no_target("src",Gst.PadDirection.SRC))
    if not bin_pad:
        sys.stderr.write(" Failed to add ghost pad in source bin \n")
        return None
    return nbin


def main(args):
    global loop, pipeline,service_id,perf_data

    number_sources = len(args)
    perf_data = PERF_DATA(number_sources)

    Gst.init(None)

    logger.debug("Creating Pipeline \n ")
    pipeline = Gst.Pipeline()
    is_live = False

    if not pipeline:
        sys.stderr.write(" Unable to create Pipeline \n")    
   

    logger.debug("Creating streamux \n ")
    streammux = Gst.ElementFactory.make("nvstreammux", "Stream-muxer")
    if not streammux:
        sys.stderr.write(" Unable to create NvStreamMux \n")
    
    pipeline.add(streammux)
    for i in range(number_sources):
        print("Creating source_bin ", i, " \n ")
        uri_name = args[i]
        if uri_name.find("rtsp://") == 0:
            is_live = True
        source_bin = create_source_bin(i, uri_name)
        if not source_bin:
            sys.stderr.write("Unable to create source bin \n")
        pipeline.add(source_bin)
        padname = "sink_%u" % i
        sinkpad = streammux.get_request_pad(padname)
        if not sinkpad:
            sys.stderr.write("Unable to create sink pad bin \n")
        srcpad = source_bin.get_static_pad("src")
        if not srcpad:
            sys.stderr.write("Unable to create src pad bin \n")
        srcpad.link(sinkpad)
    
    logger.debug("Creating Pgie \n ")
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    if not pgie:
        sys.stderr.write(" Unable to create pgie \n")

    logger.debug("Creating nvvidconv1 \n ")
    nvvidconv1 = Gst.ElementFactory.make("nvvideoconvert", "convertor1")
    if not nvvidconv1:
        sys.stderr.write(" Unable to create nvvidconv1 \n")
    
    logger.debug("Creating filter1 \n ")
    caps1 = Gst.Caps.from_string("video/x-raw(memory:NVMM), format=RGBA")
    filter1 = Gst.ElementFactory.make("capsfilter", "filter1")
    if not filter1:
        sys.stderr.write(" Unable to get the caps filter1 \n")
    filter1.set_property("caps", caps1)

    logger.debug("Creating tiler \n ")
    tiler = Gst.ElementFactory.make("nvmultistreamtiler", "nvtiler")
    if not tiler:
        sys.stderr.write(" Unable to create tiler \n")

    logger.debug("Creating nvvidconv \n ")
    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", "convertor")
    if not nvvidconv:
        sys.stderr.write(" Unable to create nvvidconv \n")
    
    logger.debug("Creating nvosd \n ")
    nvosd = Gst.ElementFactory.make("nvdsosd", "onscreendisplay")
    if not nvosd:
        sys.stderr.write(" Unable to create nvosd \n")
    
    nvvidconv_postosd = Gst.ElementFactory.make("nvvideoconvert", "convertor_postosd")
    if not nvvidconv_postosd:
        sys.stderr.write(" Unable to create nvvidconv_postosd \n")
    
    logger.debug("Creating nvtracker \n ")
    tracker = Gst.ElementFactory.make("nvtracker", "tracker")
    if not tracker:
        sys.stderr.write(" Unable to create tracker \n")
    
    logger.debug("Creating nvdsanalytics \n ")
    nvanalytics = Gst.ElementFactory.make("nvdsanalytics", "analytics")
    if not nvanalytics:
        sys.stderr.write(" Unable to create nvanalytics \n")

    nvanalytics.set_property("config-file", nvanalytics_file_path)

    # Create a caps filter
    caps = Gst.ElementFactory.make("capsfilter", "filter")
    caps.set_property("caps", Gst.Caps.from_string("video/x-raw(memory:NVMM), format=I420"))
    
    # Make the encoder
    encoder = Gst.ElementFactory.make("nvv4l2h264enc", "encoder")
    logger.debug("Creating H264 Encoder")

    if not encoder:
        sys.stderr.write(" Unable to create encoder")
    encoder.set_property('bitrate', MUXER_BATCH_TIMEOUT_USEC)
    if is_aarch64():
        encoder.set_property('preset-level', 1)
        encoder.set_property('insert-sps-pps', 1)
        #encoder.set_property('bufapi-version', 1)
    
    # Make the payload-encode video into RTP packets
    rtppay = Gst.ElementFactory.make("rtph264pay", "rtppay")
    logger.debug("Creating H264 rtppay")
  
    if not rtppay:
        sys.stderr.write(" Unable to create rtppay")
    
    # Make the UDP sink
    updsink_port_num = 5408
    sink = Gst.ElementFactory.make("udpsink", "udpsink")
    if not sink:
        sys.stderr.write(" Unable to create udpsink")
    
    sink.set_property('host', '224.221.255.255')
    sink.set_property('port', updsink_port_num)
    sink.set_property('async', False)
    sink.set_property('sync', 1)
    
    logger.debug("Playing file {} ".format(args))

    streammux.set_property('width', MUXER_OUTPUT_WIDTH)
    streammux.set_property('height', MUXER_OUTPUT_HEIGHT)
    streammux.set_property('batch-size', number_sources)
    streammux.set_property('batched-push-timeout', MUXER_BATCH_TIMEOUT_USEC)
    pgie.set_property('config-file-path', config_file_path)
    pgie_batch_size = pgie.get_property("batch-size")
    if (pgie_batch_size != number_sources):
        logger.debug("WARNING: Overriding infer-config batch-size", pgie_batch_size, " with number of sources ",number_sources, " \n")
        pgie.set_property("batch-size", number_sources)
    
    tiler_rows = int(math.sqrt(number_sources))
    tiler_columns = int(math.ceil((1.0 * number_sources) / tiler_rows))
    tiler.set_property("rows", tiler_rows)
    tiler.set_property("columns", tiler_columns)
    tiler.set_property("width", TILED_OUTPUT_WIDTH)
    tiler.set_property("height", TILED_OUTPUT_HEIGHT)

    if not is_aarch64():
        mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
        streammux.set_property("nvbuf-memory-type", mem_type)
        nvvidconv.set_property("nvbuf-memory-type", mem_type)
        nvvidconv1.set_property("nvbuf-memory-type", mem_type)
        tiler.set_property("nvbuf-memory-type", mem_type)
        nvvidconv_postosd.set_property("nvbuf-memory-type", mem_type)
    
    config = configparser.ConfigParser()
    config.read(tracker_file)
    config.sections()

    for key in config['tracker']:
        if key == 'tracker-width' :
            tracker_width = config.getint('tracker', key)
            tracker.set_property('tracker-width', tracker_width)
        if key == 'tracker-height' :
            tracker_height = config.getint('tracker', key)
            tracker.set_property('tracker-height', tracker_height)
        if key == 'gpu-id' :
            tracker_gpu_id = config.getint('tracker', key)
            tracker.set_property('gpu_id', tracker_gpu_id)
        if key == 'll-lib-file' :
            tracker_ll_lib_file = config.get('tracker', key)
            tracker.set_property('ll-lib-file', tracker_ll_lib_file)
        if key == 'll-config-file' :
            tracker_ll_config_file = config.get('tracker', key)
            tracker.set_property('ll-config-file', tracker_ll_config_file)

        
    logger.debug("Adding elements to Pipeline \n")
    pipeline.add(pgie)
    pipeline.add(tracker)
    pipeline.add(nvanalytics)
    pipeline.add(tiler)
    pipeline.add(nvvidconv)
    pipeline.add(filter1)
    pipeline.add(nvvidconv1)
    pipeline.add(nvosd)
    pipeline.add(nvvidconv_postosd)
    pipeline.add(caps)
    pipeline.add(encoder)
    pipeline.add(rtppay)
    pipeline.add(sink)

    logger.debug("Linking elements in the Pipeline \n")
    streammux.link(pgie)
    pgie.link(tracker)
    tracker.link(nvanalytics)
    nvanalytics.link(nvvidconv1)
    nvvidconv1.link(filter1)
    filter1.link(tiler)
    tiler.link(nvvidconv)
    nvvidconv.link(nvosd)
    nvosd.link(nvvidconv_postosd)
    nvvidconv_postosd.link(caps)
    caps.link(encoder)
    encoder.link(rtppay)
    rtppay.link(sink)

    # create an event loop and feed gstreamer bus mesages to it
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)
    
    # Start streaming
    rtsp_port_num = 8554
    
    server = GstRtspServer.RTSPServer.new()
    server.props.service = "%d" % rtsp_port_num
    server.attach(None)
    
    factory = GstRtspServer.RTSPMediaFactory.new()
    factory.set_launch( "( udpsrc name=pay0 port=%d buffer-size=524288 caps=\"application/x-rtp, media=video, clock-rate=90000, encoding-name=(string)%s, payload=96 \" )" % (updsink_port_num, 'H264'))
    factory.set_shared(True)

    server.get_mount_points().add_factory(f"/{service_id}", factory)
    logger.debug(f"\n *** DeepStream: Launched RTSP Streaming at rtsp://localhost:{rtsp_port_num}/{service_id} ***\n\n")

    tiler_sink_pad = tiler.get_static_pad("sink")
    if not tiler_sink_pad:
        sys.stderr.write(" Unable to get sink pad \n")
    else:
        tiler_sink_pad.add_probe(Gst.PadProbeType.BUFFER, tiler_sink_pad_buffer_probe, 0)
        # perf callback function to print fps every 5 sec
        GLib.timeout_add(5000, perf_data.perf_print_callback)


    logger.debug("Starting pipeline \n")
    # start play back and listed to events		
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    # cleanup
    logger.debug("Exiting app\n")
    pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
        for camera, info in debug_info["data"].items():
            uri_to_id_mapping[debug_info["data"][camera]['link']] = camera
        sys.exit(main(list(uri_to_id_mapping.keys())))
        
