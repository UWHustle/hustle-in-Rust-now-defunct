#ifndef QUICKSTEP_HUSTLEOPTIMIZER_H
#define QUICKSTEP_HUSTLEOPTIMIZER_H
#include <iostream>
#include <memory>

namespace quickstep {
class Catalog;

class UnableToReadCatalogE : public std::exception {
 public:
  explicit UnableToReadCatalogE(const std::string &filename)
      : message_("UnableToReadCatalog: Unable to read catalog from file ") {
    message_.append(filename);
  }

  ~UnableToReadCatalogE() throw() {
  }

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

class UnableToWriteCatalogE : public std::exception {
 public:

  explicit UnableToWriteCatalogE(const std::string &filename)
      : message_("UnableToWriteCatalog: Unable to write catalog to file ") {
    message_.append(filename);
  }

  ~UnableToWriteCatalogE() throw() {
  }

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

class CatalogNotProtoE : public std::exception {
 public:
  explicit CatalogNotProtoE(const std::string &filename)
      : message_("CatalogNotProto: The file ") {
    message_.append(filename).append(" is not valid Proto");
  }

  ~CatalogNotProtoE() throw() {
  }

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

constexpr char kCatalogPath[] = "hustlecatalog.pb.bin";

class OptimizerWrapper {
 public:
  OptimizerWrapper(){}

  std::string hustle_optimize(const std::string &sql = std::string());

 private:
  void readCatalog();

  void saveCatalog();

  void CreateDefaultCatalog();

  std::shared_ptr<quickstep::Catalog> catalog_;

  // TODO(chronis): changed shared to unique, need to figure out why the
  // catalog.hpp fails to be included here.
};


}

#endif //QUICKSTEP_HUSTLEOPTIMIZER_H
