%define component   geowave-repo
%define version     1.0
%define repo_dir    /etc/yum.repos.d
%define buildroot    %{_topdir}/BUILDROOT/%{name}-%{version}-root


Name:           %{component}
Version:        %{version}
Release:        3
BuildArch:      noarch
Summary:        GeoWave RPM Repo
Group:          Applications/Internet
License:        Apache2
Source0:        geowave.repo
BuildRoot:      %{buildroot}


%description
GeoWave RPM Repo


%prep
rm -rf %{_rpmdir}/%{buildarch}/%{name}*
rm -rf %{_srcrpmdir}/%{name}*


%build
rm -fr %{_builddir}
mkdir -p %{_builddir}/%{name}


%install
# Clean and init the directory
rm -fr %{buildroot}
mkdir -p %{buildroot}%{repo_dir}

# Unpack and rename app directory
cp %{SOURCE0} %{buildroot}%{repo_dir}


%clean
rm -fr %{buildroot}
rm -fr %{_builddir}/*


%files

%attr(644,root,root) %{repo_dir}/geowave.repo


%changelog
* Tue Feb 3 2015 Andrew Spohn <andrew.e.spohn.ctr.nga.mil> - 1.0
- First packaging
