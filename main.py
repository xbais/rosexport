# sudo apt-get install ros-rolling-tf-transformations
# pip install transforms3d
# Source ROS before running this script : source /opt/ros/rolling/setup.bash

from pathlib import Path
from rosbags.highlevel import AnyReader
from rosbags.typesys import get_types_from_msg, register_types
import numpy as np
import open3d as o3d
import struct
import os
import argparse, os
import sqlite3
import csv 
from tabulate import tabulate
from tqdm import tqdm

import numpy as np
import open3d as o3d
from pathlib import Path
from rosbags.highlevel import AnyReader
from rosbags.typesys import get_types_from_msg #, get_message
#from geometry_msgs.msg import TransformStamped
import tf_transformations
import tf2_ros
import tf2_py
#from tf2_ros.buffer_interface import TransformException
#from builtin_interfaces.msg import Time

import os
import csv
import numpy as np
from pathlib import Path
import open3d as o3d
from scipy.spatial.transform import Rotation as R
from bisect import bisect_left

def is_rosbag_ros1_or_ros2(bag_file_path):
    """
    Determine whether a given rosbag file is from ROS 1 or ROS 2.

    :param bag_file_path: Path to the rosbag file.
    :return: 'ROS 1' if the bag file is a ROS 1 bag, 'ROS 2' if it is a ROS 2 bag, 
             or 'Unknown format' if it cannot determine.
    """
    
    if not os.path.isfile(bag_file_path):
        return "Invalid bag file path"

    # Check for ROS 1 bag format
    try:
        with open(bag_file_path, 'rb') as f:
            # Read the first 4 bytes for the magic number
            magic_number = f.read(4)
            if magic_number == b"#ROSB":  # ROS 1 bag magic number
                return 'ROS 1'
            else:
                # If magic number does not match, check for ROS 2
                # ROS 2 bags are in SQLite format, we can try reading it as a SQLite database
                return check_ros2_bag(bag_file_path)
    except Exception as e:
        print(f"An error occurred while checking the bag file: {e}")
        return 'Unknown format'
    
def check_ros2_bag(bag_file_path):
    """
    Check if a given bag file is a ROS 2 bag.

    :param bag_file_path: Path to the ROS 2 bag file.
    :return: 'ROS 2' if it is a ROS 2 bag; otherwise, returns 'Unknown format'.
    """
    try:
        # Attempt to connect to the SQLite database
        conn = sqlite3.connect(bag_file_path)
        cursor = conn.cursor()

        # Check for the presence of the 'metadata' table which should be there in a ROS 2 bag
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata';")
        if cursor.fetchone():
            return 'ROS 2'
        else:
            return 'Unknown format'
    except Exception as e:
        print(f"An error occurred while checking the ROS 2 bag file: {e}")
        return 'Unknown format'
    finally:
        conn.close()



class ros2bag():
    def __init__(self, path):
        self.path = path # Folder path that contains the bag
        self.file_path = os.path.join(path,[_ for _ in os.listdir(path) if _.endswith('.mcap')][0])

        # Register PointCloud2 message type
        msg_txt = """
        std_msgs/Header header
        builtin_interfaces/Time stamp
        string frame_id
        uint32 height
        uint32 width
        sensor_msgs/PointField[] fields
        string name
        uint32 offset
        uint8 datatype
        uint32 count
        bool is_bigendian
        uint32 point_step
        uint32 row_step
        uint8[] data
        bool is_dense
        """
        try: 
            add_types = get_types_from_msg({'sensor_msgs/msg/PointCloud2': msg_txt})
            register_types(add_types)
        except Exception as e:
            print(e)

    def get_topics(self):
        topics = []
        with AnyReader([Path(self.path)]) as reader:
            for conn in reader.connections:
                topics.append((conn.topic, conn.msgtype))
        return topics
    
    
    '''
    def export_lidar(self, output_dir):
        bag_path = Path(self.path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        with AnyReader([bag_path]) as reader:
            reader.open()

            # Find PointCloud2 topics
            pc2_conns = [
                c for c in reader.connections
                if c.msgtype == 'sensor_msgs/msg/PointCloud2'
            ]

            if not pc2_conns:
                print("No PointCloud2 topics found.")
                return

            print("Found the following PointCloud2 topics:")
            for conn in pc2_conns:
                print(f" - {conn.topic}")

            count = 0
            for conn, timestamp, rawdata in reader.messages(connections=pc2_conns):
                msg = reader.deserialize(rawdata, conn.msgtype)
                points = list(point_cloud2.read_points(msg, field_names=("x", "y", "z"), skip_nans=True))

                if not points:
                    continue

                np_points = np.array(points)
                pcd = o3d.geometry.PointCloud()
                pcd.points = o3d.utility.Vector3dVector(np_points)

                topic_name = conn.topic.strip('/').replace('/', '_')
                filename = output_dir / f"{topic_name}_{count:05d}.pcd"
                o3d.io.write_point_cloud(str(filename), pcd)
                print(f"Saved: {filename} ({len(points)} points)")
                count += 1

        print(f"\n✅ Exported {count} LiDAR frames.")
        return
        '''

    # Minimal PointCloud2 parser (assumes x,y,z are first three fields)
    def read_xyz_points(self, msg, skip_nans=True):
        
        fmt = 'fff'  # x, y, z as float32
        step = msg.point_step
        data = msg.data
        num_points = len(data) // step
        points = []

        for i in range(num_points):
            offset = i * step
            x, y, z = struct.unpack_from(fmt, data, offset)
            if skip_nans and (not np.isfinite(x) or not np.isfinite(y) or not np.isfinite(z)):
                continue
            points.append((x, y, z))

        return points

    def export_lidar_from_ros2bag(self, output_dir):
        bag_path = Path(self.path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        with AnyReader([bag_path]) as reader:
            #reader.open()

            # Filter PointCloud2 topics
            pc2_conns = [
                c for c in reader.connections
                if c.msgtype == 'sensor_msgs/msg/PointCloud2'
            ]

            if not pc2_conns:
                print("❌ No PointCloud2 topics found.")
                return

            print("✅ Found PointCloud2 topics:")
            for conn in pc2_conns:
                print(f" - {conn.topic}")

            count = 0
            for conn, timestamp, rawdata in reader.messages(connections=pc2_conns):
                msg = reader.deserialize(rawdata, conn.msgtype)
                points = self.read_xyz_points(msg)
                if not points:
                    continue

                np_points = np.array(points, dtype=np.float32)
                pcd = o3d.geometry.PointCloud()
                pcd.points = o3d.utility.Vector3dVector(np_points)

                topic_name = conn.topic.strip('/').replace('/', '_')
                filename = output_dir / f"{topic_name}_{count:05d}_{str(timestamp)}.pcd"
                o3d.io.write_point_cloud(str(filename), pcd)
                print(f"💾 Saved: {filename} ({len(points)} points)")
                count += 1

        print(f"\n🎉 Done! Exported {count} LiDAR frames.")
    
    def export_odometry_from_ros2bag(self, output_csv_path):
        bag_path = Path(self.path)
        output_csv_path = Path(output_csv_path)
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)

        with AnyReader([bag_path]) as reader:
            # Filter Odometry topics
            odom_conns = [
                c for c in reader.connections
                if c.msgtype == 'nav_msgs/msg/Odometry'
            ]

            if not odom_conns:
                print("❌ No Odometry topics found.")
                return

            print("✅ Found Odometry topics:")
            for conn in odom_conns:
                print(f" - {conn.topic}")

            rows = []
            for conn, timestamp, rawdata in tqdm(reader.messages(connections=odom_conns)):
                msg = reader.deserialize(rawdata, conn.msgtype)

                # Extract timestamp in ROS2 nanoseconds
                ts_str = str(timestamp)  # Keep as string for unique filenames if needed

                # Extract position and orientation
                pos = msg.pose.pose.position
                ori = msg.pose.pose.orientation
                row = {
                    'timestamp': ts_str,
                    'x': pos.x,
                    'y': pos.y,
                    'z': pos.z,
                    'qx': ori.x,
                    'qy': ori.y,
                    'qz': ori.z,
                    'qw': ori.w
                }
                rows.append(row)

            # Write to CSV
            with open(output_csv_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            print(f"\n🎉 Done! Exported {len(rows)} odometry messages to {output_csv_path}")
    
    def load_odometry_csv(self,csv_path):
        timestamps = []
        odom_data = {}
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                timestamp = int(row['timestamp'])  # assuming nanoseconds or consistent integer format
                pose = {
                    'position': np.array([float(row['x']), float(row['y']), float(row['z'])]),
                    'orientation': np.array([float(row['qx']), float(row['qy']), float(row['qz']), float(row['qw'])])
                }
                timestamps.append(timestamp)
                odom_data[timestamp] = pose
        timestamps.sort()
        return timestamps, odom_data


    def find_nearest_timestamp(self, target_ts, sorted_ts_list):
        idx = bisect_left(sorted_ts_list, target_ts)
        if idx == 0:
            return sorted_ts_list[0]
        if idx == len(sorted_ts_list):
            return sorted_ts_list[-1]
        before = sorted_ts_list[idx - 1]
        after = sorted_ts_list[idx]
        return before if abs(target_ts - before) < abs(target_ts - after) else after


    def transform_point_cloud(self, pcd, position, orientation):
        R_mat = R.from_quat(orientation).as_matrix()
        T = np.eye(4)
        T[:3, :3] = R_mat
        T[:3, 3] = position
        return pcd.transform(T)


    def combine_lidar_scans_with_nearest_odometry(
        self,
        odom_csv_path,
        lidar_folder,
        output_path,
        lidar_topic='lidar_front',
        verbose=True
        ):
        timestamps, odom_data = self.load_odometry_csv(odom_csv_path)
        lidar_folder = Path(lidar_folder)
        merged_pcd = o3d.geometry.PointCloud()

        lidar_files = sorted(lidar_folder.glob(f"{lidar_topic}_*.pcd"))[:90]
        print(f"Total lidar clouds to merge = {len(lidar_files)}")
        percentage_to_merge = int(input("Percentage of files to merge (0 - 100) : "))

        if not lidar_files:
            print(f"❌ No LiDAR scans found for topic '{lidar_topic}' in {lidar_folder}")
            return
        print(f"Step = {int(len(lidar_files) * (percentage_to_merge/100))}")

        for pcd_file_idx in tqdm(range(0,len(lidar_files),int(len(lidar_files) / (len(lidar_files)*(percentage_to_merge/100)))), desc="Merging clouds"):
            pcd_file = lidar_files[pcd_file_idx]
            try:
                ts_str = pcd_file.stem.split('_')[-1]
                lidar_ts = int(ts_str)
            except Exception:
                if verbose:
                    print(f"⚠️ Skipping invalid filename: {pcd_file.name}")
                continue

            nearest_odom_ts = self.find_nearest_timestamp(lidar_ts, timestamps)
            pose = odom_data.get(nearest_odom_ts, None)
            if pose is None:
                if verbose:
                    print(f"⚠️ No odometry for timestamp: {nearest_odom_ts}")
                continue

            pcd = o3d.io.read_point_cloud(str(pcd_file))
            if len(pcd.points) == 0:
                if verbose:
                    print(f"⚠️ Empty point cloud: {pcd_file.name}")
                continue

            transformed = self.transform_point_cloud(pcd, pose['position'], pose['orientation'])
            merged_pcd += transformed

            if verbose:
                tqdm.write(f"✅ Merged {pcd_file.name} using odometry {nearest_odom_ts}")

        if len(merged_pcd.points) == 0:
            print("❌ No valid point clouds merged.")
            return

        o3d.io.write_point_cloud(str(output_path), merged_pcd)
        
        print(f"\n🎉 Combined LiDAR scan saved to: {output_path}")
        o3d.visualization.draw_geometries([merged_pcd])

    # --- save lidar transformed to 'world' frame

    def get_tf(self, tf_topics:list=['/tf', '/tf_static']) -> None:
        '''Lists all the available tf transforms'''
        with AnyReader([Path(self.path)]) as reader:
            #reader.open()
            for topic in tf_topics:
                tf_conns = [c for c in reader.connections if c.topic == topic]

                frames = set()
                for conn, timestamp, rawdata in reader.messages(connections=tf_conns):
                    msg = reader.deserialize(rawdata, conn.msgtype)
                    for transform in msg.transforms:
                        parent = transform.header.frame_id
                        child = transform.child_frame_id
                        frames.add((parent, child))

                print(f"⬤ TF {topic} Tree edges (parent → child):")
                for parent, child in sorted(frames):
                    print(f"{parent} → {child}")
        return

    def export_lidar_transformed(self, output_dir, world_frame='world'):
        bag_path = Path(self.path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        with AnyReader([bag_path]) as reader:
            # --- Parse only the specified LiDAR topic ---
            pc2_conns = [
                c for c in reader.connections
                if c.msgtype == 'sensor_msgs/msg/PointCloud2' and c.topic == '/husky/ouster/points'
            ]

            if not pc2_conns:
                print("❌ No PointCloud2 topics found.")
                return

            print("✅ Found PointCloud2 topic(s):")
            for conn in pc2_conns:
                print(f" - {conn.topic}")

            # --- Load TF messages over time ---
            tf_conns = [c for c in reader.connections if 'tf' in c.topic]
            tf_messages = []

            for conn, timestamp, rawdata in tqdm(reader.messages(connections=tf_conns), desc='Reading TF'):
                msg = reader.deserialize(rawdata, conn.msgtype)
                if hasattr(msg, 'transforms'):
                    for transform in msg.transforms:
                        tf_messages.append((transform, timestamp))

            # --- Build a time-aware TF buffer ---
            tf_buffer = {}
            for transform, timestamp in tf_messages:
                parent = transform.header.frame_id.strip('/')
                child = transform.child_frame_id.strip('/')
                tf_buffer.setdefault((parent, child), []).append((
                    timestamp, transform.transform.translation, transform.transform.rotation
                ))

            def get_transform_matrix(trans, rot):
                return tf_transformations.concatenate_matrices(
                    tf_transformations.translation_matrix([trans.x, trans.y, trans.z]),
                    tf_transformations.quaternion_matrix([rot.x, rot.y, rot.z, rot.w])
                )

            def invert_transform(matrix):
                return np.linalg.inv(matrix)

            def find_latest_transform(parent, child, stamp):
                key = (parent, child)
                if key not in tf_buffer:
                    return None
                # Find the latest transform before or at 'stamp'
                candidates = [t for t in tf_buffer[key] if t[0] <= stamp]
                if not candidates:
                    return None
                latest = max(candidates, key=lambda t: t[0])
                return get_transform_matrix(latest[1], latest[2])

            def compute_transform_chain(from_frame, to_frame, stamp, visited=None):
                if visited is None:
                    visited = set()

                from_frame = from_frame.strip('/')
                to_frame = to_frame.strip('/')

                if from_frame == to_frame:
                    return np.identity(4)

                visited.add(from_frame)

                for (parent, child), transforms in tf_buffer.items():
                    # Try child → parent (forward)
                    if child == from_frame and parent not in visited:
                        tfm = find_latest_transform(parent, child, stamp)
                        if tfm is None:
                            continue
                        rest = compute_transform_chain(parent, to_frame, stamp, visited)
                        if rest is not None:
                            return rest @ tfm

                    # Try parent → child (inverse)
                    if parent == from_frame and child not in visited:
                        tfm = find_latest_transform(parent, child, stamp)
                        if tfm is None:
                            continue
                        rest = compute_transform_chain(child, to_frame, stamp, visited)
                        if rest is not None:
                            return rest @ invert_transform(tfm)

                return None

            # --- Export each LiDAR frame to world ---
            count = 0
            for conn, timestamp, rawdata in tqdm(reader.messages(connections=pc2_conns), desc='Exporting'):
                msg = reader.deserialize(rawdata, conn.msgtype)
                points = self.read_xyz_points(msg)
                if not points:
                    continue

                np_points = np.array(points, dtype=np.float32)
                frame_id = msg.header.frame_id.strip('/')

                # Find full transform chain at message timestamp
                transform = compute_transform_chain(frame_id, world_frame, timestamp)
                if transform is None:
                    tqdm.write(f"⚠️ No transform to {world_frame} for time {timestamp}")
                    continue

                # Apply transformation
                ones = np.ones((np_points.shape[0], 1))
                homogenous_points = np.hstack((np_points, ones))
                transformed_points = (transform @ homogenous_points.T).T[:, :3]

                # Save to PCD
                pcd = o3d.geometry.PointCloud()
                pcd.points = o3d.utility.Vector3dVector(transformed_points)

                topic_name = conn.topic.strip('/').replace('/', '_')
                filename = output_dir / f"{topic_name}_{count:05d}_{str(timestamp)}.pcd"
                o3d.io.write_point_cloud(str(filename), pcd)
                print(f"💾 Saved: {filename} ({len(transformed_points)} points)")
                count += 1

            print(f"\n🎉 Done! Exported {count} LiDAR frames in world frame.")

        

class ros1bag():
    def __init__(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ROS Export")
    parser.add_argument("-f", "--folder", help="Location of the ROS bag export folder", type=str)
    parser.add_argument("-o", "--outdir", help="Location to save the outputs", type=str)
    args = parser.parse_args()
    path = args.folder

    # Detect the ROS version
    _ = os.listdir(path)
    _ = [os.path.join(path, __) for __ in _ if (__.endswith('db3') )]
    
    bagpath = _[0] if _ else path
    version = is_rosbag_ros1_or_ros2(bagpath)

    print(f"ROS bag version = {version}")
    
    '''
    # Setup ROS env, get the channel list
    if version == "ROS 2":
        # install ros2
        pass
    elif version  == "ROS 1":
        # install rosbag
        pass
    '''
    
    # get tops
    bag = ros2bag(path)
    tops = bag.get_topics()
    print("\n\nThe following transforms were found in the Ros2 bag")
    bag.get_tf()

    print("\n\nFollowing topics found in bag :\n", tabulate(tops))

    _msg = """
    What to export? (type corresponding number)
    1. all
    2. odometry
    3. lidar
    4. lidar in world frame\n
    """

    export_what = int(input(_msg))
    
    if export_what == 1:
        print("Functionality currently unavailable")
    elif export_what == 2:
        # Export data
        print("Exporting odometry")

        _odo_path = os.path.join(args.outdir, "odometry.csv")
        if os.path.isfile(_odo_path):
            #os.mkdir(_lidar_path)
            print("Odometry file already exists : ", _odo_path)
            _overwrite = (input("Over-write ? (y / n)").lower() == "y")
            if not _overwrite:
                exit()

        bag.export_odometry_from_ros2bag(output_csv_path=_odo_path) # '/home/aakash-remote/Desktop/bag-export'
    elif export_what == 3:
        # Export data
        print("Exporting point clouds")

        _lidar_path = os.path.join(args.outdir, "lidar")
        if not os.path.isdir(_lidar_path):
            os.mkdir(_lidar_path)
            print("New directory made : ", _lidar_path)
        bag.export_lidar_from_ros2bag(output_dir=_lidar_path) # '/home/aakash-remote/Desktop/bag-export'
    elif export_what == 4:
        print("Exporting lidar in world frame")
        _lidar_path = os.path.join(args.outdir, "lidar_world_frame")
        if not os.path.isdir(_lidar_path):
            os.mkdir(_lidar_path)
            print("New directory made : ", _lidar_path)
        
        bag.export_lidar_transformed(output_dir=_lidar_path) # '/home/aakash-remote/Desktop/bag-export'
    else:
        print("Unknown option chosen : ", export_what)

    combine_lidar_true = (input("Combine lidar with odometry? ( y / n )").lower() == "y")

    if combine_lidar_true:
        _ = ['_'.join(__.split('_')[:-2]) for __ in os.listdir(os.path.join(args.outdir, 'lidar'))]
        #print(_)
        probable_lidar_topics = sorted(list(set(_)))
        
        print("Probable lidar topics : \n", tabulate(enumerate(probable_lidar_topics)))
        selected_topic = probable_lidar_topics[int(input("Type the index of the topic from above : "))]

        bag.combine_lidar_scans_with_nearest_odometry(
            output_path=os.path.join(args.outdir, 'combined_cloud.ply'), 
            lidar_topic=selected_topic, 
            odom_csv_path=os.path.join(args.outdir, 'odometry.csv'),
            lidar_folder=os.path.join(args.outdir, 'lidar')
            )