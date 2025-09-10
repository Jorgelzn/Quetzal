# Files and folders management
import os
import shutil
import pathlib
# Needed for this missions
import cv2                    # Imagery management
import asyncio                # All tasks are asynchronous
import threading              # This class is a unique thread
from datetime import datetime # To get current date and time
import numpy as np            # To make mathematical operations
import pymap3d as pm          # Tranform from NED to WGS84 and vice versa
import airsim                 # AirSim
from mavsdk import System     # MavSDK things
import mavsdk
# General own classes for missions
from ..loggingUAV   import m, pLog          # Logging purposes
from ..telemetryUAV import drone_telemetry  # Telemetry reading and send to Django
# To connect with Django
from channels.layers import get_channel_layer # If run on Django, to connect this mission with the GUI
# Specific for this mission
from mavsdk.offboard import (OffboardError, PositionNedYaw)
import time
import json
import open3d as o3d
from .pointCloudTreatment import *
import piexif # To inject (or read) metadata of JPG images
from fractions import Fraction

import numpy as np # Scientific computing library for Python
 
def get_quaternion_from_euler(roll, pitch, yaw):
  """
  Convert an Euler angle to a quaternion.
   
  Input
    :param roll: The roll (rotation around x-axis) angle in radians.
    :param pitch: The pitch (rotation around y-axis) angle in radians.
    :param yaw: The yaw (rotation around z-axis) angle in radians.
 
  Output
    :return qx, qy, qz, qw: The orientation in quaternion [x,y,z,w] format
  """
  qx = np.sin(roll/2) * np.cos(pitch/2) * np.cos(yaw/2) - np.cos(roll/2) * np.sin(pitch/2) * np.sin(yaw/2)
  qy = np.cos(roll/2) * np.sin(pitch/2) * np.cos(yaw/2) + np.sin(roll/2) * np.cos(pitch/2) * np.sin(yaw/2)
  qz = np.cos(roll/2) * np.cos(pitch/2) * np.sin(yaw/2) - np.sin(roll/2) * np.sin(pitch/2) * np.cos(yaw/2)
  qw = np.cos(roll/2) * np.cos(pitch/2) * np.cos(yaw/2) + np.sin(roll/2) * np.sin(pitch/2) * np.sin(yaw/2)
 
  return [qx, qy, qz, qw]


#############################
# author: damigo@inf.uc3m.es
# This mission, designed on UC3M Colmenarejo Campus, 
# uses the vision parameters of the camera to project and draw over camera images the points of 
# specific coordinates. We will start by projecting the rectangle formed by the tennis court
# 1. Calibrate the camera. Intrinsic, extrinsic, etc. for the specified parameters
#   1. This may be performed in real time by flying around a set of chess boards?
# 2. Then go to specific positions and camera orientations to capture the tennis court
#   1. During the capture process it also stores the UAV rotation and position
# --------
# 1. The project process first translate the coordinates to the same putamadre

#############################

#############################################################
#                        THREADS CLASS                      #
#############################################################
class object3DReconstructionV2(threading.Thread):
    # Status variables of this Drone
    connected   = False # info of this Drone

    # Variable to communicate with MavSDK-server
    mavsdk = None
    airsim = None

    ##########################################
    ##########################################
    # Function which starts the connection with both MavSDK and AirSim
    async def messageToJS(self, message):
        await self.channel_layer.group_send(self.groupName, {'type':'messageToJS', 'drone_id': self.drone_id, 'message':'messageToPrint', 'content': message})
  
    ################################################
    ################################################
    # 
    # self.scan_altitude  - metres above terrain to perform the initial scanning
    # self.scan_radius    - radius to use on the initial scanning
    # self.scan_speed     - speed to use on the initial scanning
    # self.n_orbits       - number of orbits to perform on the initial scanning
    def __init__(self, drone_id,  droneName="", lidarName="LiDAR1", cameraName="front", goToPoint=None, scan_altitude=20, scan_radius=20, scan_speed=5, n_orbits=1):
        threading.Thread.__init__(self)

        # Init logger
        self.logFile = str(pathlib.Path(pathlib.Path(__file__).parent.resolve().parent.parent, "logs", "mission_handler.log"))
        self.pLog    = pLog(self.logFile, drone_id)

        # Store input data
        self.drone_id        = drone_id
        self.droneName       = droneName
        self.cameraName      = cameraName
        self.lidarName       = lidarName
        self.goToPoint       = goToPoint
        self.scan_altitude   = scan_altitude
        self.scan_radius     = scan_radius
        self.scan_speed      = scan_speed
        self.n_orbits        = 1 #n_orbits
        self.n_orbits_detail = 3
        self.sleep_go        = 0.25
        self.usesOffboard    = False

        # Variables to connect with Django JS website (checks if used or not, do not remove)
        try:
            self.channel_layer = get_channel_layer()
            self.groupName     = "uav"
            self.usingDjango   = True
        except: 
            self.usingDjango   = False

        # Create folders
        self.mission_path = str(pathlib.Path(pathlib.Path(__file__).parent.resolve().parent.parent, "logs", "instance_"+str(self.drone_id), "sensors"))
        if os.path.exists(self.mission_path):
            shutil.rmtree(self.mission_path) # Delete folder and its content
        os.mkdir(self.mission_path)
        self.initial_scan_path = str(pathlib.Path(self.mission_path, "recognition")) # scan folder
        if not os.path.isdir(self.initial_scan_path):
            os.mkdir(self.initial_scan_path)
        self.initial_images_path = str(pathlib.Path(self.initial_scan_path, "images"))       # images folder
        if not os.path.isdir(self.initial_images_path):
            os.mkdir(self.initial_images_path)
        self.initial_lidar_path = str(pathlib.Path(self.initial_scan_path, "lidar"))         # images folder
        if not os.path.isdir(self.initial_lidar_path):
            os.mkdir(self.initial_lidar_path)

        # Se inicializan variables de los objetos intermedios a generar
        self.camera_data          = "camera_data"
        self.objectMetadata       = "object_metadata"
        self.objectGeoMetadata    = "object_geo_metadata"
        self.initialScanName      = "initial_scan"
        self.cropScanName         = "crop_scan"
        self.segmentedScanName    = "segmented_scan"
        self.finalObjectName      = "object"
        self.recognisedObjectName = "recognised_object"

    #######################################
    # Run the mission
    def run(self):
        self.pLog.header(m.init_run)
        self.loop = asyncio.new_event_loop()              # On a loop
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.initialize())   # First connect everything, arm and takeoff the Drone
        self.loop.run_until_complete(self.mission())      # Then perform the whole mission process
        self.loop.run_until_complete(self.ending())       # Then closes safely the mission and return to home (for now?)

    #######################################
    # Initialize the drone
    async def initialize(self):
        self.pLog.header(m.init_initialize)

        # First it links with AirSim on Windows
        self.pLog.current(m.connect_to_airsim)
        self.airsim = airsim.MultirotorClient( os.environ['PX4_SIM_HOST_ADDR'] )
        self.pLog.success(m.connect_to_airsim)

        # Then with MavSDK-Server on WSL2
        self.pLog.current(m.connect_to_mavsdk)
        portBase2 = 14540
        port = portBase2 + self.drone_id - 1
        self.pLog.info(f"MavSDK port: {port}")
        self.mavsdk = System(port=50050+self.drone_id, sysid=self.drone_id+1)
        await self.mavsdk.connect(system_address=f"udp://:{port}")

        # Checks if MavSDK is correctly initialized
        async for state in self.mavsdk.core.connection_state():
            if state.is_connected:
                self.pLog.success(m.connect_to_mavsdk)
                break
        async for health in self.mavsdk.telemetry.health():
            if health.is_global_position_ok and health.is_home_position_ok:
               self.pLog.success(m.mavsdk_healthy)
               break

        # Starts all associated telemetry tasks
        self.pLog.current(m.set_var)
        try:
            self.drone_telemetry = drone_telemetry(self.pLog, cooldown=2, drone_id=self.drone_id)
            self.drone_telemetry.run_tasks(self.mavsdk)
            await asyncio.sleep(5)
            self.originLat = self.drone_telemetry.position.latitude_deg
            self.originLon = self.drone_telemetry.position.longitude_deg
            self.originHei = self.drone_telemetry.position.absolute_altitude_m
            self.pLog.success(m.set_var)
        except Exception:
            self.pLog.error(m.set_var)
            await self.close()

        # Store the initial position of the drone
        #self.pLog.current(m.get_home)

        # Arm drone
        self.pLog.current(m.drone_arm)
        try:
            await self.mavsdk.action.arm()
        except Exception:
            self.pLog.error(m.drone_arm)
            # await self.close()
        self.pLog.success(m.drone_arm)
        
        # Sets offboard initial position and starts it
        if self.usesOffboard == True:
            self.pLog.current(m.set_initial_point)
            await self.mavsdk.offboard.set_position_ned(PositionNedYaw(0.0, 0.0, 0.0, 0.0))
            self.pLog.current(m.init_offboard)
            try:
                await self.mavsdk.offboard.start()
            except Exception:
                self.pLog.error(m.init_offboard)
                await self.close()
            self.pLog.success(m.init_offboard)

    ################################################
    ################################################
    # The ending process of the mission
    async def ending(self):
        self.pLog.header(m.close)

        # Mission is finished, closes offboard mode (if used)
        if self.usesOffboard == True:
            self.pLog.current(m.close_offboard)
            try:
                await self.mavsdk.offboard.stop()
            except OffboardError as error:
                self.pLog.error(m.close_offboard)
                await self.close()
            self.pLog.success(m.close_offboard)
            await asyncio.sleep(10)

        # Return to Home and disarm
        self.pLog.current(m.drone_rtl)
        await self.mavsdk.action.return_to_launch()
        # Wait until has arrived and disarmed
        while self.drone_telemetry.is_armed:
            await asyncio.sleep(2)
            #await self.drone_telemetry.updateTelemetry(self.mavsdk)
        self.pLog.success(m.drone_rtl)
        self.pLog.header(m.close)

        # Turn off telemetry
        self.pLog.info("Closing telemetry")
        await self.drone_telemetry.close_tasks()
        self.pLog.success("Closed telemetry")

        # Turn off MavSDK-server
        self.mavsdk._stop_mavsdk_server()
        self.pLog.success("Closed MavSDK-server")
        return

    ################################################
    ################################################
    # The specific mission (in this case the 3D reconstruction data gathering)
    async def mission(self):
        self.pLog.header(m.init_mission)

        self.pLog.current("Taking off")
        await self.mavsdk.action.set_takeoff_altitude(3)
        await self.mavsdk.action.set_return_to_launch_altitude(3)
        await self.mavsdk.action.takeoff()
        await asyncio.sleep(15)         # Waits a bit for it to be fully takeoff
        self.pLog.success("Taking off")

        ##################################### PHASE 1. Going to given point
        self.pLog.current("Going to the desired place")
        if self.usingDjango == True: await self.messageToJS("Going to the desired place")
        #await self.drone_telemetry.updateTelemetry(self.mavsdk)
        flying_alt = (self.drone_telemetry.position.absolute_altitude_m - self.drone_telemetry.position.relative_altitude_m) + self.scan_altitude
        await self.go(self.goToPoint[0], self.goToPoint[1], flying_alt)
        self.pLog.success("Going to the desired place")

        ##################################### PHASE 2. Initial radial scanning
        if self.usingDjango == True: await self.messageToJS("Reached desired place. Performing the initial scanning")
        self.pLog.current("Initial scanning")
        time_one_orbit   = self.scan_radius * 2*np.pi / self.scan_speed
        recognition_time = time_one_orbit*self.n_orbits

        self.pLog.current(f"Initial scanning {time.time()}")
        await self.mavsdk.action.do_orbit(self.scan_radius, self.scan_speed, mavsdk.action.OrbitYawBehavior(0), self.goToPoint[0], self.goToPoint[1], flying_alt)
        sleepTime = 5 + (self.scan_radius / self.scan_speed) + 7 # Tiene tiempo de recibir la misión, tiempo de ir al sitio y tiempo de colocarse y comenzar la órbita. 
        
        await asyncio.sleep(sleepTime) # Wait for the drone to start the orbit. I guess we can program something better than this pls
        if self.usingDjango == True: await self.messageToJS(f"Scanning area {recognition_time} seconds for {self.n_orbits} orbits")

        # Enviar la info para que se pinte el mapa y la trayectoria a seguir
        self.pLog.current(f"Initial scanning {time.time()}")
        self.pLog.current(f"Scanning area {recognition_time} seconds for {self.n_orbits} orbits")
        fpsInitial = 0.5
        await self.orbit_scanning_camera_lidar(fpsInitial, recognition_time, scan_path=self.initial_scan_path, lidar_path=self.initial_lidar_path+"/"+self.initialScanName, camera_path=self.initial_images_path)
        self.pLog.success("Scanned area")

        ##################################### PHASE 3. Analysis to identify the objects
        if self.usingDjango == True: await self.messageToJS("Detecting objets")
        detectObjectsLidar(self, self.goToPoint, self.scan_radius, heightScan=self.drone_telemetry.position.relative_altitude_m, dbscan_eps=10, dbscan_min_points=200, ransac_max_distance=1, ransac_min_plane_points=50, ransac_num_iter=5000)
        # Enviar la info a JS para que la pinte en el mapa

        ##################################### PHASE 4. Detailed scanning per detected object
        objects_dirs = []
        for folder in os.listdir(self.initial_scan_path):
            if folder.startswith("object_"): 
                objects_dirs.append(self.initial_scan_path+"/"+folder)

        print("-- objects detected: ", len(objects_dirs))
        print("-- Starting individual scans")
        for i in range(len(objects_dirs)):
            print("-- Scanning object: ", i)
            # Read the object's position
            filePath = objects_dirs[i]+"/"+self.objectMetadata+".txt"
            with open(filePath, "r") as file:
                lines = file.readlines()
            object_lat    = float(lines[1])
            object_lon    = float(lines[2])
            object_alt    = float(lines[3])
            object_width  = float(lines[4])
            radius_offset = 5
            area_radius   = object_width+radius_offset
            height_offset = 0 #object_alt/2 # Height offset over the object's height
            target_alt    = (self.drone_telemetry.position.absolute_altitude_m - self.drone_telemetry.position.relative_altitude_m) + object_alt + height_offset # Terreno + altura del objeto + offset de altura

            #go in survey altitude to a safe distance to object (to evade collisions)    
            r_earth           = 6371000
            object_radius_lat = object_lat + (area_radius / r_earth) * (180 / np.pi)
            await self.go(object_radius_lat, object_lon, flying_alt)
            
            # parameters
            minAltitude = 1.5                                                   # min altitude above the ground
            groundLimit = (self.drone_telemetry.position.absolute_altitude_m - self.drone_telemetry.position.relative_altitude_m) + minAltitude # set the limit of the ground altitude
            #if self.n_orbits_detail > 1:
            #    jump             = (target_alt-groundLimit) / (self.n_orbits_detail-1)   # altitude jump between orbits
            #else: 
            #    jump = 0
            jump = 2 # vertical jump between orbits
            self.n_orbits_detail = int(np.ceil((target_alt-groundLimit)/jump))
            n_fotos          = 20                                                    # number of photos to take per orbit
            scan_speed       = 1                                                     # m/s as speed of the drone
            time_offset      = 10                                                    # additional seconds to wait before taking the first photo
            time_one_orbit   = (area_radius * 2 * np.pi) / scan_speed                # Se calcula aprox el tiempo en el que se realiza una órbita
            recognition_time = time_one_orbit + time_offset                          # Se calcula el tiempo total de reconocimiento
            fps              = n_fotos / recognition_time                            # Se calcula la cantidad de fps a obtener
            arrLidarFiles    = []

            for j in range(self.n_orbits_detail):             #loop for orbits in multiple altitudes until reaching limit 
                self.pLog.current("Scanning object "+str(i)+" orbit number "+str(j))
                self.pLog.current("Placing for orbit nº"+str(j))
                outputFileLiDAR = objects_dirs[i]+"/"+"lidar"+"/"+"orbit"+str(j)
                arrLidarFiles.append( outputFileLiDAR )
                # Se coloca en la posición inicial de la órbita actual
                await self.go(self.drone_telemetry.position.latitude_deg, self.drone_telemetry.position.longitude_deg, target_alt)
                self.pLog.success("Placing for orbit nº"+str(j))
                # Comienza la órbita con su sleep asociado. Durante dicho tiempo se toman las fotos y LiDAR
                self.pLog.current("Orbiting centered on: "+str(object_lat)+" "+str(object_lon)+" "+str(target_alt)+" with radius "+str(area_radius)+" and speed "+str(scan_speed))
                await self.mavsdk.action.do_orbit(area_radius, scan_speed, mavsdk.action.OrbitYawBehavior(0), object_lat, object_lon, object_alt)
                await asyncio.sleep(recognition_time)
                await self.orbit_scanning_camera_lidar(fps, recognition_time, scan_path=objects_dirs[i], lidar_path=outputFileLiDAR, camera_path=objects_dirs[i]+"/"+"images"+"/"+"orbit"+str(j))
                self.pLog.success("Scanning object "+str(i)+" orbit number "+str(j))
                # Al final de la órbita, se vuelve baja a la siguiente altura
                target_alt -= jump
                if target_alt < groundLimit:
                    break

            print("-- Object: ", str(i), ". Scanned")
            #after each scan, go up for survey altitude to evade colisions
            await self.go(self.drone_telemetry.position.latitude_deg, self.drone_telemetry.position.longitude_deg, flying_alt)

        # at the end ending() is applied
        #self.finalProcessing(objects_dirs) # The object generation applied here? Or at the ground station?

    ##################
    # Tras obtener el dataset, en la "ground station" se procesa la info para lograr los objetos 3D y metadatos
    def finalProcessing(self, objects_dirs):
        print("-- Starting objects processing")
        for i in range(len(objects_dirs)):
            # merge PCD files. Source: https://sungchenhsi.medium.com/adding-pointcloud-to-pointcloud-9bf035707273
            arrPCD = []
            pcd_combined = o3d.geometry.PointCloud()
            for j in range(len(arrLidarFiles)):
                pcd = o3d.io.read_point_cloud(arrLidarFiles[j]+".ply")
                arrPCD.append( np.asarray(pcd.points) )
            # 
            mergedPoints = np.concatenate( arrPCD, axis=0 )
            pcd_combined.points = o3d.utility.Vector3dVector( mergedPoints )
            a = objects_dirs[i]+"/"+"lidar"+"/"+"/"+"merged.ply"
            o3d.io.write_point_cloud(a, pcd_combined)

            print("-- Processing object",i)
            meshGeneration(object_dir[i], origin)
            print("-- Classifying object")
            image_classifier.classify(object_dir)


    ################################################
    ################################################
    # Llevar al dron a un punto 3D determinado
    # pos_range es en latitud y longitud
    # alt_range es en metros
    async def go(self, lat, lon, alt, pos_range=0.0001, alt_range=0.1):
        self.pLog.current(f"Going to location -  Lat: {lat} Lon: {lon} Alt: {alt}")
        await self.mavsdk.action.goto_location(lat, lon, alt, 0)

        timeForLog = 20
        lastUpdate = time.time()
        while True:
            await asyncio.sleep(self.sleep_go)
            if time.time() > lastUpdate + timeForLog:
                self.pLog.current(f"Currently at -  Lat: {self.drone_telemetry.position.latitude_deg} Lon: {self.drone_telemetry.position.longitude_deg} Alt: {self.drone_telemetry.position.absolute_altitude_m}")
            #await self.drone_telemetry.updateTelemetry(self.mavsdk)
            if (np.abs(lat)-pos_range <= np.abs(self.drone_telemetry.position.latitude_deg)        <= np.abs(lat)+pos_range and
                np.abs(lon)-pos_range <= np.abs(self.drone_telemetry.position.longitude_deg)       <= np.abs(lon)+pos_range and
                np.abs(alt)-alt_range <= np.abs(self.drone_telemetry.position.absolute_altitude_m) <= np.abs(alt)+alt_range):
                self.pLog.success(f"Location reached!")
                break

    ################################################
    # 
    def getCameraSettings(self):
        pathSettings = pathlib.Path(os.path.expanduser("~"), "AirSimSettings", "settings.json")
        with open(pathSettings, 'r') as fp:
            data = json.load(fp)
        
        ### Lidar
        lidar_settings = data['Vehicles'][self.droneName]['Sensors'][self.lidarName]
        # Get lidar extrinsics
        lidar_extrinsics = {}
        lidar_extrinsics['X']     = lidar_settings['X']
        lidar_extrinsics['Y']     = lidar_settings['Y']
        lidar_extrinsics['Z']     = lidar_settings['Z']
        lidar_extrinsics['Pitch'] = lidar_settings['Pitch']
        lidar_extrinsics['Roll']  = lidar_settings['Roll']
        lidar_extrinsics['Yaw']   = lidar_settings['Yaw']

        ### Camera
        capture_settings = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['CaptureSettings'][0]
        # Get the camera intrinsics
        img_width  = int(capture_settings['Width'])
        img_height = int(capture_settings['Height'])
        img_fov    = capture_settings['FOV_Degrees']
        # Compute the focal length
        fov_rad = img_fov * np.pi/180
        fdy     = img_width  / (np.tan(fov_rad/2.0) * 2)
        fdx     = img_height / (np.tan(fov_rad/2.0) * 2)
        # Create the camera intrinsic object
        camera_intrinsics = o3d.camera.PinholeCameraIntrinsic()
        camera_intrinsics.set_intrinsics(img_width, img_height, fdx, fdy, img_width/2, img_height/2)

        # Get the camera extrinsics
        camera_extrinsics = {}
        camera_extrinsics['X']     = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['X']
        camera_extrinsics['Y']     = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['Y']
        camera_extrinsics['Z']     = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['Z']
        camera_extrinsics['Pitch'] = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['Pitch']
        camera_extrinsics['Roll']  = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['Roll']
        camera_extrinsics['Yaw']   = data['Vehicles'][self.droneName]['Cameras'][self.cameraName]['Yaw']

        return camera_intrinsics, camera_extrinsics, lidar_extrinsics

    ################################################
    ################################################
    # Initial scanning
    async def orbit_scanning_camera_lidar(self, fps, limit_time, scan_path="", lidar_path="", camera_path=""):
        # if the folder doesn't exist, create it
        if not os.path.exists(scan_path):
            os.makedirs(scan_path)
        if not os.path.exists(camera_path):
            os.makedirs(camera_path)
        
            # create file for camera metadata
        camara_data_path = scan_path+"/"+self.camera_data+".csv"
        with open(camara_data_path, 'w') as camera_data_file:
            camera_data_file.write("img_name, wQuaternion, yQuaternion, zQuaternion, xQuaternion, latitude_deg, longitude_deg, absolute_altitude_m, pitch_deg, roll_deg, yaw_deg\n")

        # Get camera settings
        camera_intrinsics, camera_extrinsics, lidar_extrinsics = self.getCameraSettings()
        
        # Variable to store the point cloud
        pcd = o3d.geometry.PointCloud()

        self.pLog.current("Scanning loop")
        init_time    = time.time()
        actual_frame = 0
        clock        = 0
        img_number   = 0
        while clock < limit_time:
            #GET LIDAR DATA
            lidar_data = self.airsim.getLidarData(lidar_name=self.lidarName, vehicle_name=self.droneName)
            
            #GET IMAGE
            img_name   = "image_"+str(img_number)+".jpg"
            image_path = camera_path+"/"+img_name
            # Ask AirSim for snapshots
            responses = self.airsim.simGetImages([airsim.ImageRequest(self.cameraName, airsim.ImageType.Scene, False, False)])
            photo     = np.frombuffer(responses[0].image_data_uint8, dtype=np.uint8)
            img       = photo.reshape(responses[0].height, responses[0].width, 3)
            img_number += 1

            # 
            if clock - actual_frame > (1/fps):
                actual_frame = clock
            # small sleep to avoid overloading the CPU
            await asyncio.sleep(self.sleep_go)

            # Add metadata and save the image
            self.snapshot_with_metadata(img, image_path)

            # Store the image metadata in the csv file
            with open(camara_data_path, 'a') as camera_data_file:
                camera_data_file.write("%s, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n" %(img_name, 
                self.drone_telemetry.quaternion.w,           self.drone_telemetry.quaternion.y,           self.drone_telemetry.quaternion.z, self.drone_telemetry.quaternion.x,
                self.drone_telemetry.position.latitude_deg,  self.drone_telemetry.position.longitude_deg, self.drone_telemetry.position.absolute_altitude_m,
                self.drone_telemetry.uav_attitude.pitch_deg, self.drone_telemetry.uav_attitude.roll_deg,  self.drone_telemetry.uav_attitude.yaw_deg))

            # Drone position regarding the origin
            droneX, droneY, droneZ = pm.geodetic2enu(lat0=self.originLat, lon0=self.originLon, h0=self.originHei, lat=self.drone_telemetry.position.latitude_deg, lon=self.drone_telemetry.position.longitude_deg, h=self.drone_telemetry.position.absolute_altitude_m)            
            lidarX = droneX + lidar_extrinsics['X']
            lidarY = droneY + lidar_extrinsics['Y']
            lidarZ = droneZ + lidar_extrinsics['Z']
            lidarPitch = self.drone_telemetry.uav_attitude.pitch_deg #+ lidar_extrinsics['Pitch']
            lidarRoll  = self.drone_telemetry.uav_attitude.roll_deg  #+ lidar_extrinsics['Roll']
            lidarYaw   = self.drone_telemetry.uav_attitude.yaw_deg   #+ lidar_extrinsics['Yaw']
            lidarqW = self.drone_telemetry.quaternion.w
            lidarqX = self.drone_telemetry.quaternion.x
            lidarqY = self.drone_telemetry.quaternion.y
            lidarqZ = self.drone_telemetry.quaternion.z
            lidarQX, lidarQY, lidarQZ, lidarQW = get_quaternion_from_euler(roll=lidarRoll, pitch=lidarPitch, yaw=lidarYaw)
            cameraX = droneX + camera_extrinsics['X']
            cameraY = droneY + camera_extrinsics['Y']
            cameraZ = droneZ + camera_extrinsics['Z']
            cameraPitch = self.drone_telemetry.uav_attitude.pitch_deg #+ camera_extrinsics['Pitch']
            cameraRoll  = self.drone_telemetry.uav_attitude.roll_deg  #+ camera_extrinsics['Roll']
            cameraYaw   = self.drone_telemetry.uav_attitude.yaw_deg   #+ camera_extrinsics['Yaw']
            cameraqW = self.drone_telemetry.quaternion.w
            cameraqX = self.drone_telemetry.quaternion.x
            cameraqY = self.drone_telemetry.quaternion.y
            cameraqZ = self.drone_telemetry.quaternion.z
            cameraQX, cameraQY, cameraQZ, cameraQW = get_quaternion_from_euler(roll=cameraRoll, pitch=cameraPitch, yaw=cameraYaw)

            print("Drone position MavSDK XYZ: ", droneX, droneY, droneZ)

            print("Lidar position MavSDK XYZ: ", lidarX, lidarY, lidarZ)
            print("Lidar position AirSim XYZ: ", lidar_data.pose.position.x_val,lidar_data.pose.position.y_val,lidar_data.pose.position.z_val)
            print("Lidar rotation MavSDK PRY: ",  lidarPitch, lidarRoll, lidarYaw)
            print("Lidar rotation MavSDK WXYZ: ", lidarQW, lidarQX, lidarQY, lidarQZ)
            print("Lidar rotation MavAAA WXYZ: ", lidarqW, lidarqX, lidarqY, lidarqZ)
            print("Lidar rotation AirSim WXYZ: ", lidar_data.pose.orientation.w_val, lidar_data.pose.orientation.x_val, lidar_data.pose.orientation.y_val, lidar_data.pose.orientation.z_val)

            print("Camera position MavSDK XYZ: ",  cameraX, cameraY, cameraZ)
            print("Camera position AirSim XYZ: ",  responses[0].camera_position.x_val, responses[0].camera_position.y_val, responses[0].camera_position.z_val)
            print("Camera rotation MavSDK PRY: ",  lidarPitch, lidarRoll, lidarYaw)
            print("Camera rotation MavSDK WXYZ: ", cameraQW, cameraQX, cameraQY, cameraQZ)
            print("Camera rotation MavAAA WXYZ: ", cameraqW, cameraqX, cameraqY, cameraqZ)
            print("Camera rotation AirSim WXYZ: ", responses[0].camera_orientation.w_val, responses[0].camera_orientation.x_val, responses[0].camera_orientation.y_val, responses[0].camera_orientation.z_val)


            #LIDAR EXTRINSICS
            # Hay que encontrar cómo añadir las extrínsecas de la cámara y el lidar que están en euler a la MavAAA WXYZ
            #qw, qx, qy, qz    = lidar_data.pose.orientation.w_val, lidar_data.pose.orientation.x_val, lidar_data.pose.orientation.y_val, lidar_data.pose.orientation.z_val
            qw, qx, qy, qz    = lidarQW, lidarQX, lidarQY, lidarQZ
            lidar_rotation    = o3d.geometry.get_rotation_matrix_from_quaternion((qw, qx, qy, qz)) # wxyz
            # o3d.geometry.get_rotation_matrix_from_xyz
            lidar_translation_xyz = [lidarX, lidarY, lidarZ] # XYZ
            #CAMERA EXTRINSICS
            #qw, qx, qy, qz     = responses[0].camera_orientation.w_val, responses[0].camera_orientation.x_val, responses[0].camera_orientation.y_val, responses[0].camera_orientation.z_val
            qw, qx, qy, qz    = cameraQW, cameraQX, cameraQY, cameraQZ
            camera_rotation    = o3d.geometry.get_rotation_matrix_from_quaternion((qw, qy, qz, qx)) # wyzx
            camera_translation_yzx = [cameraY, cameraZ, cameraX] # YZX

            #build the colorized point cloud
            img = img[...,::-1]   # brg to rgb
            for i in range(0, len(lidar_data.point_cloud), 3):
                # Point cloud to YZX
                xyz = lidar_data.point_cloud[i:i+3]                 # Gets a LiDAR point
                xyz = lidar_rotation.dot(xyz)+lidar_translation_xyz # transformation from lidar pose to world
                yzx = np.array([xyz[1],xyz[2],xyz[0]])              # from xyz to yzx on world

                # Get the color for the point
                projection  = np.linalg.inv(camera_rotation).dot(yzx - camera_translation_yzx)
                coordinateX = -1 * camera_intrinsics.get_focal_length()[0] * projection[0] / projection[2]
                coordinateY = -1 * camera_intrinsics.get_focal_length()[1] * projection[1] / projection[2]
                pixel       = [int(coordinateX+camera_intrinsics.width/2), int(camera_intrinsics.height/2-coordinateY)]

                # Store the colour, only if the point is inside the image
                if pixel[1] > 0 and pixel[1] < camera_intrinsics.height and \
                   pixel[0] > 0 and pixel[0] < camera_intrinsics.width:
                    # Create the point cloud
                    point = o3d.geometry.PointCloud()
                    b = np.array(yzx)
                    point.points = o3d.utility.Vector3dVector( np.reshape( b, (1,3) ) )
                    a = np.array(img[pixel[1]][pixel[0]]/255)
                    point.colors = o3d.utility.Vector3dVector( np.reshape( a, (1,3) ) )
                    pcd += point # Add these points to the full point cloud

            clock = time.time()-init_time # To check if still has to take pictures
            #await asyncio.sleep(0.1)
            
        # save the point cloud
        pointcloud_path = lidar_path+".ply"
        o3d.io.write_point_cloud(pointcloud_path, pcd)
        self.pLog.success("Scanning finished")

    ######################
    # Perform the snapshot and store it with metadata
    def snapshot_with_metadata(self, img, image_path):
        # Calculate lat-lon references
        lat = self.decdeg2dms(self.drone_telemetry.position.latitude_deg)  # latitude 
        lon = self.decdeg2dms(self.drone_telemetry.position.longitude_deg) # longitude
        north_south_reference = "N"
        if lat[0] < 0:
            north_south_reference = "S"
            lat = [-1 * x for x in lat]
        east_west_reference = "E"
        if lon[0] < 0:
            east_west_reference = "W"
            lon = [-1 * x for x in lon]
        
        # Other metadata
        speedModule = (self.drone_telemetry.posandvel.velocity.north_m_s**2 + self.drone_telemetry.posandvel.velocity.east_m_s**2 + self.drone_telemetry.posandvel.velocity.down_m_s**2)**0.5
        yaw = self.drone_telemetry.uav_attitude.yaw_deg + 180

        # Date and timestamp
        dateTimeObj = datetime.now()                                       # data
        dateTimeObj.timestamp()
        day         = int(dateTimeObj.timestamp() / 86400) * 86400
        hour        = int((dateTimeObj.timestamp() - day) / 3600)
        minutes     = int((dateTimeObj.timestamp() - day - hour * 3600) / 60)
        seconds     = int(dateTimeObj.timestamp()  - day - hour * 3600 - minutes * 60)
        day_timestamp_str = dateTimeObj.strftime("%Y:%m:%d")
        
        gps_ifd = {
            piexif.GPSIFD.GPSAltitudeRef:     0,
            piexif.GPSIFD.GPSAltitude:        (int(self.drone_telemetry.position.absolute_altitude_m),1),
            piexif.GPSIFD.GPSLatitudeRef:     north_south_reference,
            piexif.GPSIFD.GPSLatitude:        ((lat[0], 1), (lat[1], 1), (lat[2], 10000000)),
            piexif.GPSIFD.GPSLongitudeRef:    east_west_reference,
            piexif.GPSIFD.GPSLongitude:       ((lon[0], 1), (lon[1], 1), (lon[2], 10000000)),
            piexif.GPSIFD.GPSImgDirectionRef: "T",
            piexif.GPSIFD.GPSImgDirection:    (int(yaw*100), 100),
            piexif.GPSIFD.GPSTimeStamp:       [(hour, 1), (minutes, 1), (seconds, 1)], 
            piexif.GPSIFD.GPSDateStamp:       day_timestamp_str,
            piexif.GPSIFD.GPSSpeed:           (int(speedModule), 1), # 3.6 Km/h == 1 m/s
            piexif.GPSIFD.GPSSpeedRef:        "K",
        }
        exif_bytes = piexif.dump({"GPS": gps_ifd})

        # Save the image
        cv2.imwrite(              image_path, img)
        piexif.insert(exif_bytes, image_path)

    #########################
    # Auxiliar function transforming lat-lon from DD (decimal degrees) to DMS (degrees minutes seconds)
    def decdeg2dms(self, dd):
        mult = -1 if dd < 0 else 1
        mnt,sec = divmod(abs(dd)*3600, 60)
        deg,mnt = divmod(mnt, 60)
        return int(mult*deg), int(mult*mnt), int(mult*sec*10000000)