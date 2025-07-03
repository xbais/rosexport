from pathlib import Path
from rosbags.highlevel import AnyReader
from rosbags.typesys import get_types_from_msg, register_types
import numpy as np
import open3d as o3d
import struct
import os
import argparse, os


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

class ros1bag():
    def __init__(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ROS Export")
    parser.add_argument("--file", help="Location of the ROS bag", type=str)
    args = parser.parse_args()
    path = args.file

    # Detect the ROS version
    _ = os.listdir(path)
    _ = [os.path.join(path, __) for __ in _ if __.endswith('db3') ]
    bagpath = _[0]
    version = is_rosbag_ros1_or_ros2(bagpath)

    print(f"ROS bag version = {version}")
    
    # Setup ROS env, get the channel list
    if version == "ROS 2":
        # install ros2
        pass
    elif version  == "ROS 1":
        # install rosbag
        pass

    # get tops
    bag = ros2bag(path)
    tops = bag.get_topics()
    print(tops)

    # Export data
    print("Exporting point clouds")
    bag.export_lidar_from_ros2bag(output_dir='/home/aakash-remote/Desktop/bag-export')
    #os.system("source /opt/ros/rolling/setup.bash && ros2 bag info /xnet/aakash_rosbags/rosbag2_2025_06_17-11_00_56/ --topic-name")


