#include <geometry_msgs/TransformStamped.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <pcl/io/pcd_io.h>
#include <regex>
#include <filesystem>

#include "lidar_align/loader.h"
#include "lidar_align/transform.h"

namespace fs = std::filesystem;

namespace lidar_align
{

  Loader::Loader(const Config &config) : config_(config) {}

  Loader::Config Loader::getConfig(ros::NodeHandle *nh)
  {
    Loader::Config config;
    nh->param("use_n_scans", config.use_n_scans, config.use_n_scans);
    return config;
  }

  void Loader::parsePointcloudMsg(const sensor_msgs::PointCloud2 msg,
                                  LoaderPointcloud *pointcloud)
  {
    bool has_timing = false;
    bool has_intensity = false;
    for (const sensor_msgs::PointField &field : msg.fields)
    {
      if (field.name == "time_offset_us")
      {
        has_timing = true;
      }
      else if (field.name == "intensity")
      {
        has_intensity = true;
      }
    }

    if (has_timing)
    {
      pcl::fromROSMsg(msg, *pointcloud);
      return;
    }
    else if (has_intensity)
    {
      Pointcloud raw_pointcloud;
      pcl::fromROSMsg(msg, raw_pointcloud);

      for (const Point &raw_point : raw_pointcloud)
      {
        PointAllFields point;
        point.x = raw_point.x;
        point.y = raw_point.y;
        point.z = raw_point.z;
        point.intensity = raw_point.intensity;

        if (!std::isfinite(point.x) || !std::isfinite(point.y) ||
            !std::isfinite(point.z) || !std::isfinite(point.intensity))
        {
          continue;
        }

        pointcloud->push_back(point);
      }
      pointcloud->header = raw_pointcloud.header;
    }
    else
    {
      pcl::PointCloud<pcl::PointXYZ> raw_pointcloud;
      pcl::fromROSMsg(msg, raw_pointcloud);

      for (const pcl::PointXYZ &raw_point : raw_pointcloud)
      {
        PointAllFields point;
        point.x = raw_point.x;
        point.y = raw_point.y;
        point.z = raw_point.z;

        if (!std::isfinite(point.x) || !std::isfinite(point.y) ||
            !std::isfinite(point.z))
        {
          continue;
        }

        pointcloud->push_back(point);
      }
      pointcloud->header = raw_pointcloud.header;
    }
  }

  bool Loader::loadPointcloudFromROSBag(const std::string &bag_path,
                                        const Scan::Config &scan_config,
                                        Lidar *lidar)
  {
    rosbag::Bag bag;
    try
    {
      bag.open(bag_path, rosbag::bagmode::Read);
    }
    catch (rosbag::BagException e)
    {
      ROS_ERROR_STREAM("LOADING BAG FAILED: " << e.what());
      return false;
    }

    std::vector<std::string> types;
    types.push_back(std::string("sensor_msgs/PointCloud2"));
    rosbag::View view(bag, rosbag::TypeQuery(types));

    size_t scan_num = 0;
    for (const rosbag::MessageInstance &m : view)
    {
      std::cout << " Loading scan: \e[1m" << scan_num++ << "\e[0m from ros bag"
                << '\r' << std::flush;

      LoaderPointcloud pointcloud;
      parsePointcloudMsg(*(m.instantiate<sensor_msgs::PointCloud2>()),
                         &pointcloud);

      lidar->addPointcloud(pointcloud, scan_config);

      if (lidar->getNumberOfScans() >= config_.use_n_scans)
      {
        break;
      }
    }
    if (lidar->getTotalPoints() == 0)
    {
      ROS_ERROR_STREAM(
          "No points were loaded, verify that the bag contains populated "
          "messages of type sensor_msgs/PointCloud2");
      return false;
    }

    return true;
  }

  std::vector<std::string> Loader::_findFilesByPattern(const std::string &directoryPath, const std::string &filePatternStr)
  {
    std::vector<std::string> filePaths;
    std::regex filePattern(filePatternStr);

    for (const auto &entry : fs::directory_iterator(directoryPath))
    {
      if (fs::is_regular_file(entry) && std::regex_match(entry.path().filename().string(), filePattern))
      // if (fs::is_regular_file(entry) && entry.path().filename().string().ends_with(filePatternStr))
      {
        filePaths.push_back(entry.path().string());
      }
    }

    auto naturalCompare = [&filePattern](const std::string &a, const std::string &b)
    {
      std::smatch matchA, matchB;
      bool aMatch = std::regex_search(a, matchA, filePattern);
      bool bMatch = std::regex_search(b, matchB, filePattern);

      if (aMatch && bMatch)
      {
        int numA = std::stoi(matchA[1]);
        int numB = std::stoi(matchB[1]);

        return numA < numB; // For descending order
      }

      return false; // Default case, should not happen if all files match the pattern
    };

    std::sort(filePaths.begin(), filePaths.end(), naturalCompare);

    return filePaths;
  }

  bool Loader::loadPointcloudFromPcdDir(const std::string &pcd_dir, const Scan::Config &scan_config, Lidar *lidar)
  {
    //
    auto pcd_paths = _findFilesByPattern(pcd_dir, "([0-9]+)\\.pcd");

    size_t scan_num = 0;
    for (const auto &path : pcd_paths)
    {
      std::cout << " Loading scan: \e[1m" << scan_num++ << "\e[0m from ros bag" << '\r' << std::flush;

      pcl::PointCloud<PointAllFields>::Ptr pointcloud(new LoaderPointcloud());
      if (pcl::io::loadPCDFile<PointAllFields>(path, *pointcloud) == -1)
      {
        ROS_ERROR_STREAM("Error while reading the file!\n");
        return (-1);
      }

      lidar->addPointcloud(*pointcloud, scan_config);

      if (lidar->getNumberOfScans() >= config_.use_n_scans)
      {
        break;
      }
    }
    if (lidar->getTotalPoints() == 0)
    {
      ROS_ERROR_STREAM(
          "No points were loaded, verify that the bag contains populated "
          "messages of type sensor_msgs/PointCloud2");
      return false;
    }

    return true;
  }

  bool Loader::loadTformFromROSBag(const std::string &bag_path, Odom *odom)
  {
    rosbag::Bag bag;
    try
    {
      bag.open(bag_path, rosbag::bagmode::Read);
    }
    catch (rosbag::BagException e)
    {
      ROS_ERROR_STREAM("LOADING BAG FAILED: " << e.what());
      return false;
    }

    std::vector<std::string> types;
    types.push_back(std::string("geometry_msgs/TransformStamped"));
    rosbag::View view(bag, rosbag::TypeQuery(types));

    size_t tform_num = 0;
    for (const rosbag::MessageInstance &m : view)
    {
      std::cout << " Loading transform: \e[1m" << tform_num++
                << "\e[0m from ros bag" << '\r' << std::flush;

      geometry_msgs::TransformStamped transform_msg =
          *(m.instantiate<geometry_msgs::TransformStamped>());

      Timestamp stamp = transform_msg.header.stamp.sec * 1000000ll +
                        transform_msg.header.stamp.nsec / 1000ll;

      Transform T(Transform::Translation(transform_msg.transform.translation.x,
                                         transform_msg.transform.translation.y,
                                         transform_msg.transform.translation.z),
                  Transform::Rotation(transform_msg.transform.rotation.w,
                                      transform_msg.transform.rotation.x,
                                      transform_msg.transform.rotation.y,
                                      transform_msg.transform.rotation.z));
      odom->addTransformData(stamp, T);
    }

    if (odom->empty())
    {
      ROS_ERROR_STREAM("No odom messages found!");
      return false;
    }

    return true;
  }

  bool Loader::loadTformFromMaplabCSV(const std::string &csv_path, Odom *odom)
  {
    std::ifstream file(csv_path, std::ifstream::in);

    size_t tform_num = 0;
    while (file.peek() != EOF)
    {
      std::cout << " Loading transform: \e[1m" << tform_num++
                << "\e[0m from csv file" << '\r' << std::flush;

      Timestamp stamp;
      Transform T;

      if (getNextCSVTransform(file, &stamp, &T))
      {
        odom->addTransformData(stamp, T);
      }
    }

    return true;
  }

  bool Loader::loadTfromFromTxt(const std::string &txt_path, Odom *odom)
  {
    //
    std::ifstream inf(txt_path, std::ifstream::in);

    int id;
    std::string timestamp;
    Eigen::Vector3d xyz;
    Eigen::Quaterniond q;

    while (inf >> id >> timestamp >> xyz[0] >> xyz[1] >> xyz[2] >> q.x() >> q.y() >> q.z() >> q.w())
    {
      Timestamp stamp = std::stoll(timestamp) / 1000ll;
      Transform T = Transform(Transform::Translation(xyz[0], xyz[1], xyz[2]), Transform::Rotation(q.w(), q.x(), q.y(), q.z()));

      odom->addTransformData(stamp, T);
    }

    //
    return true;
  }

  // lots of potential failure cases not checked
  bool Loader::getNextCSVTransform(std::istream &str, Timestamp *stamp,
                                   Transform *T)
  {
    std::string line;
    std::getline(str, line);

    // ignore comment lines
    if (line[0] == '#')
    {
      return false;
    }

    std::stringstream line_stream(line);
    std::string cell;

    std::vector<std::string> data;
    while (std::getline(line_stream, cell, ','))
    {
      data.push_back(cell);
    }

    if (data.size() < 9)
    {
      return false;
    }

    constexpr size_t TIME = 0;
    constexpr size_t X = 2;
    constexpr size_t Y = 3;
    constexpr size_t Z = 4;
    constexpr size_t RW = 5;
    constexpr size_t RX = 6;
    constexpr size_t RY = 7;
    constexpr size_t RZ = 8;

    *stamp = std::stoll(data[TIME]) / 1000ll;
    *T = Transform(Transform::Translation(std::stod(data[X]), std::stod(data[Y]),
                                          std::stod(data[Z])),
                   Transform::Rotation(std::stod(data[RW]), std::stod(data[RX]),
                                       std::stod(data[RY]), std::stod(data[RZ])));

    return true;
  }

} // namespace lidar_align
