#pragma once
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <iostream>
#include <map>
#include <cctype>

class Keplerian_TemplateBank_Reader {
private:
  std::vector<double> n;   // angular velocity = 2pi / orbital period
  std::vector<double> a1;                 // projected semi-major axis in light seconds
  std::vector<double> phi;                // orbital phase
  std::vector<double> omega;              // longitude of periastron
  std::vector<double> ecc;                // eccentricity
  int columns = 0;

  // store every “# KEY: VALUE” line here
  std::map<std::string, std::string> metadata;

  static bool is_comment(const std::string& line) {
    return !line.empty() && line[0] == '#';
  }

  static std::vector<std::string> split(const std::string& s) {
    std::stringstream ss(s);
    return {std::istream_iterator<std::string>(ss), std::istream_iterator<std::string>()};
  }

  // Helper: trim whitespace from front/back
  static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
  }

public:
  Keplerian_TemplateBank_Reader(const std::string& filename) {
    load(filename);
  }

  void load(const std::string& filename) {
    std::ifstream in(filename);
    ErrorChecker::check_file_error(in, filename);
    std::string line;
    while (std::getline(in, line)) {
      if (is_comment(line)) {
        // Remove leading '#', then parse "KEY: VALUE"
        std::string rest = line.substr(1);
        rest = trim(rest);
        auto colon = rest.find(':');
        if (colon != std::string::npos) {
          std::string key   = trim(rest.substr(0, colon));
          std::string value = trim(rest.substr(colon + 1));
          if (!key.empty()) {
            metadata[key] = value;
          }
        }
        continue;
      }
      auto tokens = split(line);
      if (columns == 0) columns = tokens.size();
      if (tokens.size() == 3) {
        n.push_back(std::stod(tokens[0]));
        a1.push_back(std::stod(tokens[1]));
        phi.push_back(std::stod(tokens[2]));
        omega.push_back(0.0);
        ecc.push_back(0.0);
      } else if (tokens.size() == 5) {
        n.push_back(std::stod(tokens[0]));
        a1.push_back(std::stod(tokens[1]));
        phi.push_back(std::stod(tokens[2]));
        omega.push_back(std::stod(tokens[3]));
        ecc.push_back(std::stod(tokens[4]));
      } else {
        throw std::runtime_error("Template_Bank: Invalid line with " + std::to_string(tokens.size()) + " columns. Expected 3 or 5.");
      }
    }
  }

  const std::vector<double>& get_n() const { return n; }
  const std::vector<double>& get_a1() const { return a1; }
  const std::vector<double>& get_phi() const { return phi; }
  const std::vector<double>& get_omega() const { return omega; }
  const std::vector<double>& get_ecc() const { return ecc; }
  int get_num_columns() const { return columns; }
  const std::map<std::string, std::string>& get_metadata() const { return metadata; }
};
