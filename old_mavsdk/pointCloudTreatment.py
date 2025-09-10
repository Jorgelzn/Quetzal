import open3d as o3d
import numpy as np
import os
import pymap3d as pm          # Tranform from NED to WGS84 and vice versa

import fiona
from fiona.crs import from_epsg
import os
from shapely.geometry import MultiPolygon, Polygon, mapping, Point

import math
import copy

########################################################
########################################################
########################################################
########################################################
# This class performs the point cloud treatment
########################################################
########################################################
########################################################
########################################################


#####################
# Read the point cloud
def read(file):
    print(" Loading point cloud...")
    pcd = o3d.io.read_point_cloud(file)
    print(" ",pcd)
    return pcd

#####################
# Crop the point cloud
# Del centro, hace un cuadrado en plano XY y se lo queda
def crop(pcd, output, range=5):
    pcd_center = pcd.get_center()
    a = np.array(pcd_center)-range
    b = np.array(pcd_center)+range
    c = o3d.geometry.AxisAlignedBoundingBox(a, b)
    pcdCropped        = pcd.crop( c )
    o3d.io.write_point_cloud(output, pcdCropped)
    return pcdCropped

####################
# apply ransac algorithm to get only the non-ground points
# explanation: https://blog.karthisoftek.com/a?ID=01800-e2bdd164-2810-467b-b5d6-d7d83a0b7da3
# maxDist: maximum distance between two points to be considered as inlier
# ransac_n: number of points in a plane that is estimated using random sampling
# num_iter: number of iterations of the ransac algorithm
########################## MEJORES SOLUCIONES PARA SEGMENTAR EL SUELO
# https://www.researchgate.net/post/Ground_point_extraction_filters_or_algorithms
# https://www.danielgm.net/cc/
# Tiene librería para Python https://github.com/CloudCompare/CloudComPy
def plane_segmentation(pcd, output, distance=0.2, ransac=50, iter=5000):
    plane_model, inliers = pcd.segment_plane(distance_threshold=distance, ransac_n=ransac, num_iterations=iter)
    #inlier_cloud         = pcd.select_by_index(inliers)
    outlier_cloud        = pcd.select_by_index(inliers, invert=True)
    o3d.io.write_point_cloud(output, outlier_cloud)
    return outlier_cloud
  
####################
# Le aplica rotación y escala
def applyMeshRotationAndScale(mesh):
    R = mesh.get_rotation_matrix_from_xyz((np.pi,0,0))
    mesh.rotate(R, center=mesh.get_center())                    #invert
    mesh.scale(100, center=mesh.get_center())                   #scale
    return mesh

####################
# translate origin to center of object
def translateOriginToCenterOfObject(mesh):
    mesh_box = mesh.get_axis_aligned_bounding_box()
    altura   = abs(mesh_box.get_min_bound()-mesh_box.get_max_bound())[2]
    mesh.translate((0, 0, altura/2),relative=False)
    return mesh

##################
# Alpha shape mesh reconstruction
def alpha_shape(pcd, output, alpha=0.7):
    print(" Alpha shape...")
    mesh = o3d.geometry.TriangleMesh.create_from_point_cloud_alpha_shape(pcd, alpha)
    mesh.compute_vertex_normals()
    o3d.io.write_triangle_mesh(output, mesh)
    print(" Alpha shape done")
    return mesh

##################
# Ball pivoting mesh reconstruction
def ball_pivoting(pcd, output, radii=np.arange(0.1, 0.2, 0.05), radi=1.5, nn=1000):
    print(" Ball pivoting...")
    # Estimating normals
    pcd.estimate_normals(search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=radi, max_nn=nn))
    # Orienting normals
    pcd.orient_normals_towards_camera_location(pcd.get_center())
    normals     = np.asarray(pcd.normals)
    # flip normals
    pcd.normals = o3d.utility.Vector3dVector(-normals)
    # Creating mesh
    mesh        = o3d.geometry.TriangleMesh.create_from_point_cloud_ball_pivoting(pcd, o3d.utility.DoubleVector(radii))
    o3d.io.write_triangle_mesh(output, mesh)
    print(" Ball pivoting done")
    return mesh

####################
# Poisson surface mesh reconstruction
def poisson_surface(pcd, output, depth=15, radi=1.5, nn=1000):
    print(" Poisson surface...")
    box = pcd.get_axis_aligned_bounding_box()
    for i in range( int(len(pcd.points)/100) ): # create ground under object
        area  = [np.random.uniform(box.get_min_bound()[0],box.get_max_bound()[0]),np.random.uniform(box.get_min_bound()[1],box.get_max_bound()[1]),box.get_max_bound()[2]]
        point = o3d.geometry.PointCloud()
        point.points = o3d.utility.Vector3dVector(np.reshape(np.array(area),(1,3)))
        #point.colors = o3d.utility.Vector3dVector(np.reshape(np.array(pcd.colors[np.random.randint(0,len(pcd.points))]),(1,3)))
        pcd += point

    # Estimating normals
    pcd.estimate_normals(search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=radi, max_nn=nn))
    # Orienting normals
    pcd.orient_normals_towards_camera_location(pcd.get_center())
    normals          = np.asarray(pcd.normals)
    # flip normals
    pcd.normals      = o3d.utility.Vector3dVector(-normals)
    # Creating mesh
    mesh_, densities = o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(pcd,depth)
    o3d.io.write_triangle_mesh(output, mesh_)
    print(" Poisson surface done")
    return mesh_


############
# Create a SHP polygon from a list of points
def createSHPPolygon(path="", name="", geometry=[], id=0, epsg=4326, metadata=None):
    SHPExtension = ".shp"
    schema = {
        'geometry': 'Polygon',
        'properties': { # Crear esto de forma dinámica con un dict que reciba
            'id':         'int',
            #'Altitude':   'float',
            #'Width':      'float',
        },
    }
    fileOutputPath = path+"/"+name+"_polygon"+SHPExtension
    outFile        = fiona.open(fileOutputPath, mode = 'w', crs=from_epsg(epsg), driver='ESRI Shapefile', schema=schema)
    geometry       = Polygon(geometry) # longitud, latitud (AL REVES VAYA)
    outFile.write({
        'geometry': mapping(geometry),
        'properties': {
            'id': id,
        },
    })
    outFile.close()

############
# Create a SHP polygon from a list of points
def createSHPPoint(path="", name="", id=0, latitude=0, longitude=0, epsg=4326, metadata=None):
    SHPExtension = ".shp"
    schema = {
        'geometry': 'Point',
        'properties': { # Crear esto de forma dinámica con un dict que reciba
            'id':         'int',
            #'Altitude':   'float',
            #'Width':      'float',
        },
    }
    fileOutputPath = path+"/"+name+"_center"+SHPExtension
    outFile        = fiona.open(fileOutputPath, mode = 'w', crs=from_epsg(epsg), driver='ESRI Shapefile', schema=schema)
    geometry       = Point([latitude, longitude]) # longitud, latitud (AL REVES VAYA)
    outFile.write({
        'geometry': mapping(geometry),
        'properties': {
            'id': id,
        },
    })
    outFile.close()

##################
# Perform the object identificacion and positioning using scanned point cloud
def detectObjectsLidar(missionSelf, origin, scan_range, heightScan=0, dbscan_eps=10, dbscan_min_points=200, ransac_max_distance=0.7, ransac_min_plane_points=50, ransac_num_iter=5000):
    missionSelf.pLog.header(f"Starting detectObjectsLidar")
    scan_path           = os.path.join(missionSelf.initial_lidar_path, missionSelf.initialScanName  +".ply")
    crop_scan_path      = os.path.join(missionSelf.initial_lidar_path, missionSelf.cropScanName     +".ply")
    segmented_scan_path = os.path.join(missionSelf.initial_lidar_path, missionSelf.segmentedScanName+".ply")

    # Read the scanned point cloud
    pcd = read(scan_path)
    # pointcloud initial segmentation
    missionSelf.pLog.current(f"Segmenting the point cloud")
    
    # Se rota la nube de puntos (necesario?¿)
    xRotation = 0
    yRotation = 0
    zRotation = 0
    rotation = (math.radians(xRotation), math.radians(yRotation), math.radians(zRotation))
    pcdRotated = copy.deepcopy(pcd)
    pcdRotated.rotate(pcd.get_rotation_matrix_from_xyz(rotation))
    rotated_path           = os.path.join(missionSelf.initial_lidar_path, "rotated"  +".ply")
    o3d.io.write_point_cloud(rotated_path, pcdRotated)

    pcdCropped   = crop(pcdRotated, crop_scan_path, range=scan_range/2)
    pcdSegmented = plane_segmentation(pcdCropped, segmented_scan_path, distance=1.2, ransac=ransac_min_plane_points, iter=ransac_num_iter)
    missionSelf.pLog.success(f"Segmenting the point cloud")

    # Identifying objects in the point cloud
    missionSelf.pLog.current(f"Identifying objects in the point cloud")
    labels    = np.array(pcdSegmented.cluster_dbscan(eps=10, min_points=dbscan_min_points))
    labels_id = np.unique(labels)
    objects   = [o3d.geometry.PointCloud() for i in range(len(labels_id))]
    # Add each point to its corresponding object
    for i in range(len(labels)):
        point        = o3d.geometry.PointCloud()
        point.points = o3d.utility.Vector3dVector(np.reshape(np.array(pcdSegmented.points[i]),(1,3)))
        #point.colors = o3d.utility.Vector3dVector(np.reshape(np.array(pcdSegmented.colors[i]),(1,3)))
        objects[labels[i]] += point
    missionSelf.pLog.success(f"Identifying objects in the point cloud")

    # on each detected object
    for i in range(len(objects)):
        missionSelf.pLog.current(f"Processing detected object nº{i}")

        # Store the detected object
        object_path = missionSelf.initial_scan_path+"/"+"object_"+str(i)
        os.mkdir(object_path)
        os.mkdir(object_path+"/"+"lidar")
        os.mkdir(object_path+"/"+"images")
        filePath = object_path+"/"+missionSelf.recognisedObjectName+".ply"
        o3d.io.write_point_cloud(filePath, objects[i])

        # Extract object's shape metadata
        box         = objects[i].get_axis_aligned_bounding_box()
        anchura     = max(abs(box.get_min_bound()-box.get_max_bound())[:2])
        altura      = abs(box.get_min_bound()[2])
        center      = objects[i].get_center()
        minPos      = box.get_min_bound()
        maxPos      = box.get_max_bound()
        # Se transforman esas posiciones a LatLon
        print("Object "+str(i))
        print("Center: "+str(center))
        print("MinPos: "+str(minPos))
        print("MaxPos: "+str(maxPos))
        print("X axis dimensions: "+str(abs(minPos[0]-maxPos[0])))
        print("Y axis dimensions: "+str(abs(minPos[1]-maxPos[1])))
        print("Z axis dimensions: "+str(abs(minPos[2]-maxPos[2])))
        print()
        center_data = pm.ned2geodetic(center[0], center[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
        min_data    = pm.ned2geodetic(minPos[0], minPos[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
        max_data    = pm.ned2geodetic(maxPos[0], maxPos[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
        square = [[min_data[1], min_data[0]], [min_data[1], max_data[0]], [max_data[1], max_data[0]], [max_data[1], min_data[0]]]
        # store the object metadata in a TXT file
        fileOutputPath = object_path+"/"+missionSelf.objectMetadata+".txt"
        file = open(fileOutputPath, 'w')
        file.write("latitud | longitud | altura | orbita | clase\n%f\n%f\n%f\n%f\n" % (center_data[0], center_data[1], altura, anchura))
        file.close()

        # create a SHP for QGIS
        createSHPPolygon(path=object_path, name=missionSelf.objectMetadata+str(i), geometry=square, id=i, epsg=4326, metadata=None)
        createSHPPoint(  path=object_path, name=missionSelf.objectMetadata+str(i), latitude=center_data[1], longitude=center_data[0], id=i, epsg=4326, metadata=None)
        missionSelf.pLog.success(f"Processing detected object nº{i}")

        print("END recognition")

#############
# make 3d object
def meshGeneration(pcdSegmented, object_path=""):
    object_mesh_path = object_path+"/"+"alpha_shape"+".obj"
    as_alpha = 0.7
    alpha_shape(pcdSegmented, object_mesh_path, alpha=as_alpha)
    
    object_mesh_path = object_path+"/"+"ball_pivoting"+".obj"
    bp_radii = np.arange(0.1, 0.2, 0.05)
    bp_radi  = 1.5
    bp_nn    = 1000
    ball_pivoting(pcdSegmented, object_mesh_path, radii=bp_radii, radi=bp_radi, nn=bp_nn)

    object_mesh_path = object_path+"/"+"poisson_surface"+".obj"
    ps_depth = 15
    ps_radi  = 1.5
    ps_nn    = 1000
    poisson_surface(pcdSegmented, object_mesh_path, depth=ps_depth, radi=ps_radi, nn=ps_nn)

################
# Recibe la nube de puntos y el origen de la misma, y extrae la geolocalización de la misma
def exportGeolocation(pcdSegmented, origin, height, outputPath, outputName, metadata):
    # pillar dimensiones segmentado
    geobox = pcdSegmented.get_axis_aligned_bounding_box()
    center = pcdSegmented.get_center()
    minPos = geobox.get_min_bound()
    maxPos = geobox.get_max_bound()

    # Se obtiene la geolocalización
    # En vez de exportar un polygon con el máx y min, sacarlo con un voxel desde una vista top-down? http://www.open3d.org/docs/0.14.1/tutorial/geometry/voxelization.html
    origin = []
    origin.append( 40.544167 ) 
    origin.append( -4.011747 ) 
    altura      = abs(geobox.get_min_bound()[2])
    heightScan  = 0
    center_data = pm.ned2geodetic(center[0], center[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
    min_data    = pm.ned2geodetic(minPos[0], minPos[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
    max_data    = pm.ned2geodetic(maxPos[0], maxPos[1], altura, origin[0], origin[1], heightScan, ell=None, deg=True)
    square = [[min_data[1], min_data[0]], [min_data[1], max_data[0]], [max_data[1], max_data[0]], [max_data[1], min_data[0]]]
    createSHPPoint(  path=outputPath, name=outputName, latitude=center_data[1], longitude=center_data[0], epsg=4326, metadata=None)
    createSHPPolygon(path=outputPath, name=outputName, geometry=square, epsg=4326, metadata=None)
    